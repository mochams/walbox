"""Unit tests for the receiver/consumer split and bounded backpressure.

Pure, no Postgres: a fake `_transport` with a scriptable `read()`, and a
`handler` gated by a test-controlled `asyncio.Event`.
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
    """A `ReplicationTransport` stand-in with a scriptable `read()`."""

    frames: list[bytes] = field(default_factory=list)

    async def connect(self) -> None:
        pass

    async def create_slot_if_missing(self) -> None:
        pass

    async def start_replication(self, start_lsn: int) -> None:
        pass

    async def read(self) -> bytes:
        return self.frames.pop(0)

    async def write(self, payload: bytes) -> None:
        pass


def _options(
    *,
    max_pending_transactions: int = 100,
    status_interval: int = 10,
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


def _client(**kwargs: int) -> ReplicationClient:
    """A `ReplicationClient` ready for direct `_enqueue`-family unit tests.

    `_next_status_at` is normally only set for real by `_run_once`; a test
    that calls `_enqueue`/`_await_with_status_updates`
    directly bypasses that, so it starts out already "due." Pushing the
    deadline far into the future here avoids a spurious status update firing
    -- and needing a real `_transport` -- partway through an assertion that
    has nothing to do with status updates.
    """
    client = ReplicationClient(_options(**kwargs))
    client._transport = AsyncMock()
    client._next_status_at = time.monotonic() + 1000
    return client


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


async def test_enqueue_uses_put_nowait_when_the_queue_has_room():
    client = _client(max_pending_transactions=2)
    transaction = _transaction()

    async def _fail_if_called(item: Transaction) -> None:
        pytest.fail("put() should not be used when put_nowait() would succeed")

    client._queue.put = _fail_if_called

    await client._enqueue(transaction)

    assert client._queue.get_nowait() is transaction


async def test_enqueue_falls_back_to_blocking_put_when_the_queue_is_full():
    client = _client(max_pending_transactions=1)
    await client._enqueue(_transaction(xid=1))

    second = _transaction(xid=2)
    blocked = asyncio.ensure_future(client._enqueue(second))
    await asyncio.sleep(0)
    assert not blocked.done()

    first = client._queue.get_nowait()
    assert first.xid == 1

    await asyncio.wait_for(blocked, timeout=1.0)
    assert client._queue.get_nowait() is second


async def test_backpressured_receiver_still_sends_status_updates():
    client = ReplicationClient(_options(max_pending_transactions=1, status_interval=1))
    client._transport = AsyncMock()
    client._next_status_at = time.monotonic()  # already "due"
    await client._enqueue(_transaction(xid=1))  # fills the queue (room=1)

    blocked = asyncio.ensure_future(client._enqueue(_transaction(xid=2)))
    await asyncio.sleep(0.05)

    assert not blocked.done()
    client._transport.write.assert_awaited()

    blocked.cancel()
    with pytest.raises(asyncio.CancelledError):
        await blocked


async def test_await_with_status_updates_never_duplicates_the_underlying_task():
    client = ReplicationClient(_options(status_interval=1))
    client._transport = AsyncMock()
    client._next_status_at = time.monotonic()  # already "due"
    calls = 0
    release = asyncio.Event()

    async def _slow() -> str:
        nonlocal calls
        calls += 1
        await release.wait()
        return "done"

    task = asyncio.ensure_future(client._await_with_status_updates(_slow()))
    await asyncio.sleep(1.1)  # long enough for at least two status-update laps

    assert calls == 1
    assert client._transport.write.await_count >= 2

    release.set()
    assert await asyncio.wait_for(task, timeout=1.0) == "done"


async def test_close_unblocks_a_receiver_blocked_on_a_full_queue():
    client = _client(max_pending_transactions=1)
    await client._enqueue(_transaction(xid=1))

    blocked = asyncio.ensure_future(client._enqueue(_transaction(xid=2)))
    await asyncio.sleep(0)
    assert not blocked.done()

    client.close()

    with pytest.raises(asyncio.QueueShutDown):
        await asyncio.wait_for(blocked, timeout=1.0)


async def test_close_unblocks_an_idle_consumer():
    client = ReplicationClient(_options())
    handler = AsyncMock()

    consumer = asyncio.ensure_future(client._consume_loop(handler))
    await asyncio.sleep(0)
    assert not consumer.done()

    client.close()

    await asyncio.wait_for(consumer, timeout=1.0)
    handler.assert_not_awaited()
