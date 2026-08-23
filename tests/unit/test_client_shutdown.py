"""Unit tests for graceful shutdown.

Pure, no Postgres: a fake `_transport` with a scriptable `read()` and
call-order recording, plus a `handler` gated by a test-controlled
`asyncio.Event`.
"""

import asyncio
import time
from dataclasses import dataclass
from dataclasses import field
from unittest.mock import AsyncMock

import pytest

from walbox.abc import ReplicationOptions
from walbox.abc import Transaction
from walbox.client import ReplicationClient
from walbox.errors import ReplicationConnectionError


@dataclass
class _FakeCheckpointStore:
    """A minimal `CheckpointStore` stand-in, no disk/DB involved."""

    checkpoint_lsn: int | None = None
    saved: list[int] = field(default_factory=list)

    async def load(self) -> int | None:
        return self.checkpoint_lsn

    async def save(self, lsn: int, *, connection: object | None = None) -> None:
        self.saved.append(lsn)


@dataclass
class _FakeTransport:
    """A `ReplicationTransport` stand-in with a scriptable `read()`.

    Records the order in which `end_copy`/`close`/`write` are called, plus
    when the receiver and consumer loops themselves exit, so tests can
    assert ordering across the whole shutdown sequence.
    """

    frames: list[bytes] = field(default_factory=list)
    idle: bool = False
    call_order: list[str] = field(default_factory=list)
    write_calls: list[bytes] = field(default_factory=list)

    async def connect(self) -> None:
        pass

    async def create_slot_if_missing(self) -> None:
        pass

    async def start_replication(self, start_lsn: int) -> None:
        pass

    async def read(self) -> bytes:
        if self.frames:
            return self.frames.pop(0)
        if self.idle:
            await (
                asyncio.Event().wait()
            )  # never resolves; caller re-awaits via status updates
        raise AssertionError("no more frames scripted")

    async def write(self, payload: bytes) -> None:
        self.write_calls.append(payload)
        self.call_order.append("write")

    async def end_copy(self) -> None:
        self.call_order.append("end_copy")

    def close(self) -> None:
        self.call_order.append("close")


def _options(
    *,
    max_pending_transactions: int = 100,
    status_interval: float = 10,
) -> ReplicationOptions:
    return ReplicationOptions(
        consumer_name="test-consumer",
        dsn="postgresql://example",
        slot_name="test_slot",
        publication_name="test_pub",
        checkpoint_store=_FakeCheckpointStore(),
        max_pending_transactions=max_pending_transactions,
        status_interval=status_interval,
    )


def _transaction(xid: int = 1) -> Transaction:
    return Transaction(xid=xid, commit_lsn=xid * 100, commit_time=0)


def _xlog_payload(inner: bytes, wal_start: int = 1, wal_end: int = 1) -> bytes:
    return (
        b"w"
        + wal_start.to_bytes(8, "big")
        + wal_end.to_bytes(8, "big")
        + (0).to_bytes(8, "big")
        + inner
    )


def _begin_inner(xid: int, final_lsn: int, commit_time: int = 111) -> bytes:
    return (
        b"B"
        + final_lsn.to_bytes(8, "big")
        + commit_time.to_bytes(8, "big")
        + xid.to_bytes(4, "big")
    )


def _commit_inner(commit_lsn: int, end_lsn: int, commit_time: int = 222) -> bytes:
    return (
        b"C"
        + (0).to_bytes(1, "big")
        + commit_lsn.to_bytes(8, "big")
        + end_lsn.to_bytes(8, "big")
        + commit_time.to_bytes(8, "big")
    )


async def test_receiver_notices_closing_and_exits_within_one_status_interval():
    status_interval = 0.05
    client = ReplicationClient(_options(status_interval=status_interval))
    client._transport = _FakeTransport(idle=True)
    client._next_status_at = time.monotonic() + status_interval

    receiver = asyncio.ensure_future(client._receive_loop())
    await asyncio.sleep(0)
    assert not receiver.done()

    client.close()

    await asyncio.wait_for(receiver, timeout=status_interval * 5)


async def test_run_once_sends_a_final_status_update_after_both_loops_stop():
    transport = _FakeTransport(idle=True)
    client = ReplicationClient(_options(status_interval=0.05))
    client._new_transport = lambda: transport

    original_receive_loop = client._receive_loop
    original_consume_loop = client._consume_loop

    async def _receive_loop() -> None:
        await original_receive_loop()
        transport.call_order.append("receiver_stopped")

    async def _consume_loop(handler) -> None:
        await original_consume_loop(handler)
        transport.call_order.append("consumer_stopped")

    client._receive_loop = _receive_loop
    client._consume_loop = _consume_loop

    run_task = asyncio.ensure_future(client.run(AsyncMock()))
    await asyncio.sleep(0.1)  # let both loops start and settle idle

    client.close()
    await asyncio.wait_for(run_task, timeout=1.0)

    end_copy_index = transport.call_order.index("end_copy")
    final_write_index = end_copy_index - 1
    assert transport.call_order[final_write_index] == "write"
    assert "receiver_stopped" in transport.call_order[:final_write_index]
    assert "consumer_stopped" in transport.call_order[:final_write_index]


async def test_run_once_ends_the_copy_stream_before_closing_the_transport():
    transport = _FakeTransport(idle=True)
    client = ReplicationClient(_options(status_interval=0.05))
    client._new_transport = lambda: transport

    run_task = asyncio.ensure_future(client.run(AsyncMock()))
    await asyncio.sleep(0.1)

    client.close()
    await asyncio.wait_for(run_task, timeout=1.0)

    assert transport.call_order[-3:] == ["write", "end_copy", "close"]


async def test_run_returns_normally_on_a_clean_shutdown():
    transport = _FakeTransport(idle=True)
    client = ReplicationClient(_options(status_interval=0.05))
    client._new_transport = lambda: transport

    run_task = asyncio.ensure_future(client.run(AsyncMock()))
    await asyncio.sleep(0.1)

    client.close()

    assert await asyncio.wait_for(run_task, timeout=1.0) is None


async def test_in_flight_handler_completes_and_checkpoints_before_run_returns():
    frames = [
        _xlog_payload(_begin_inner(xid=1, final_lsn=100)),
        _xlog_payload(_commit_inner(commit_lsn=100, end_lsn=150)),
    ]
    transport = _FakeTransport(frames=frames, idle=True)
    checkpoint_store = _FakeCheckpointStore()
    options = _options(status_interval=0.05)
    options.checkpoint_store = checkpoint_store
    client = ReplicationClient(options)
    client._new_transport = lambda: transport

    started = asyncio.Event()
    release = asyncio.Event()
    side_effects: list[int] = []

    async def handler(transaction: Transaction) -> None:
        started.set()
        await release.wait()
        side_effects.append(transaction.xid)

    run_task = asyncio.ensure_future(client.run(handler))
    await asyncio.wait_for(started.wait(), timeout=1.0)

    client.close()
    await asyncio.sleep(
        0.05
    )  # give close() a moment to (wrongly) short-circuit, if it would
    assert not run_task.done()
    assert side_effects == []

    release.set()
    await asyncio.wait_for(run_task, timeout=1.0)

    assert side_effects == [1]
    # commit_lsn on the assembled Transaction is end_lsn - 1 (walbox/transaction.py).
    assert checkpoint_store.saved == [149]


async def test_reconnect_is_not_attempted_when_closing_during_a_connection_error():
    client = ReplicationClient(_options())

    async def _fail(handler) -> None:
        # Simulates close() racing in from another task right as the
        # connection drops -- by the time run() inspects `_closing`, it's
        # already set.
        client._closing.set()
        raise ReplicationConnectionError("boom")

    client._run_once = _fail
    sleep_calls = []

    async def _fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)

    original_sleep = asyncio.sleep
    asyncio.sleep = _fake_sleep
    try:
        await asyncio.wait_for(client.run(AsyncMock()), timeout=1.0)
    finally:
        asyncio.sleep = original_sleep

    assert sleep_calls == []
