"""Unit tests for reconnect/backoff orchestration in `ReplicationClient.run`.

Pure, no Postgres: a fake `ReplicationTransport` double drives the outer
retry loop and its interaction with `_run_once`, with `asyncio.sleep`
monkeypatched to a no-op so tests don't actually wait through backoff
delays.
"""

import asyncio
from dataclasses import dataclass
from dataclasses import field
from unittest.mock import AsyncMock

import pytest

from walbox.abc import ReplicationOptions
from walbox.client import ReplicationClient
from walbox.client import _next_backoff_value
from walbox.errors import DecodeError
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


def _options() -> ReplicationOptions:
    return ReplicationOptions(
        consumer_name="test-consumer",
        dsn="postgresql://example",
        slot_name="test_slot",
        publication_name="test_pub",
        checkpoint_store=_FakeCheckpointStore(),
    )


def _xlog_payload(
    inner: bytes,
    wal_start: int = 1,
    wal_end: int = 1,
    send_time: int = 0,
) -> bytes:
    return (
        b"w"
        + wal_start.to_bytes(8, "big")
        + wal_end.to_bytes(8, "big")
        + send_time.to_bytes(8, "big")
        + inner
    )


def _begin_inner(final_lsn: int = 100, commit_time: int = 111, xid: int = 1) -> bytes:
    return (
        b"B"
        + final_lsn.to_bytes(8, "big")
        + commit_time.to_bytes(8, "big")
        + xid.to_bytes(4, "big")
    )


def _commit_inner(
    flags: int = 0,
    commit_lsn: int = 100,
    end_lsn: int = 150,
    commit_time: int = 222,
) -> bytes:
    return (
        b"C"
        + bytes([flags])
        + commit_lsn.to_bytes(8, "big")
        + end_lsn.to_bytes(8, "big")
        + commit_time.to_bytes(8, "big")
    )


@dataclass
class _FakeTransport:
    """A `ReplicationTransport` stand-in whose `connect`/`read` behavior is
    scripted per test, so retry/backoff orchestration can be exercised
    without a real socket or Postgres.
    """

    connect_fail_times: int = 0
    frames: list[bytes] = field(default_factory=list)
    connect_calls: int = 0
    start_replication_calls: int = 0
    read_calls: int = 0

    async def connect(self) -> None:
        self.connect_calls += 1
        if self.connect_calls <= self.connect_fail_times:
            message = "simulated connect failure"
            raise ReplicationConnectionError(message)

    async def create_slot_if_missing(self) -> None:
        pass

    async def start_replication(self, start_lsn: int) -> None:
        self.start_replication_calls += 1

    async def read(self) -> bytes:
        self.read_calls += 1
        if not self.frames:
            raise StopAsyncIteration
        return self.frames.pop(0)

    async def write(self, payload: bytes) -> None:
        pass


def test_next_backoff_value_doubles_up_to_the_cap():
    assert _next_backoff_value(1.0) == 2.0
    assert _next_backoff_value(2.0) == 4.0
    assert _next_backoff_value(4.0) == 8.0
    assert _next_backoff_value(8.0) == 16.0
    assert _next_backoff_value(16.0) == 32.0
    assert _next_backoff_value(32.0) == 60.0
    assert _next_backoff_value(40.0) == 60.0
    assert _next_backoff_value(60.0) == 60.0


async def test_run_retries_on_replication_connection_error_and_eventually_succeeds(
    monkeypatch,
):
    monkeypatch.setattr(asyncio, "sleep", AsyncMock())
    transport = _FakeTransport(
        connect_fail_times=2,
        frames=[_xlog_payload(_begin_inner()), _xlog_payload(_commit_inner())],
    )
    client = ReplicationClient(_options())
    client._new_transport = lambda: transport
    handler = AsyncMock()

    with pytest.raises(ExceptionGroup):
        await client.run(handler)

    assert transport.connect_calls == 3
    handler.assert_awaited_once()
    (delivered,), _kwargs = handler.await_args
    assert delivered.xid == 1


async def test_non_connection_errors_are_not_retried():
    transport = _FakeTransport(frames=[_xlog_payload(b"Z")])
    client = ReplicationClient(_options())
    client._new_transport = lambda: transport
    handler = AsyncMock()

    with pytest.raises(ExceptionGroup) as exc_info:
        await client.run(handler)

    assert isinstance(exc_info.value.exceptions[0], DecodeError)
    assert transport.connect_calls == 1


async def test_handler_exceptions_are_not_retried():
    transport = _FakeTransport(
        frames=[_xlog_payload(_begin_inner()), _xlog_payload(_commit_inner())],
    )
    client = ReplicationClient(_options())
    client._new_transport = lambda: transport

    async def handler(transaction: object) -> None:
        raise ValueError(transaction)

    with pytest.raises(ExceptionGroup) as exc_info:
        await client.run(handler)

    assert isinstance(exc_info.value.exceptions[0], ValueError)
    assert transport.connect_calls == 1


async def test_backoff_resets_after_a_successful_run_once(monkeypatch):
    sleeps: list[float] = []

    async def _fake_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)
    transport = _FakeTransport(connect_fail_times=1)
    client = ReplicationClient(_options())
    client._new_transport = lambda: transport

    original_read = transport.read

    async def _read_then_drop_then_stop() -> bytes:
        if transport.read_calls == 0:
            transport.read_calls += 1
            message = "simulated mid-stream drop"
            raise ReplicationConnectionError(message)
        return await original_read()

    transport.read = _read_then_drop_then_stop

    with pytest.raises(ExceptionGroup):
        await client.run(AsyncMock())

    assert transport.connect_calls == 3
    assert sleeps == [1.0, 1.0]
