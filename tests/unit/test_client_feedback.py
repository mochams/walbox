"""Unit tests for replication feedback: the durable-progress hook and the
`_durable_lsn`-derived status updates it feeds.

Pure, no Postgres: a fake `CheckpointStore` double and a stub `_transport`,
no real filesystem or network I/O.
"""

from dataclasses import dataclass
from dataclasses import field
from unittest.mock import AsyncMock

import pytest

from walbox.abc import CheckpointHandle
from walbox.abc import WalboxOptions
from walbox.client import Client
from walbox.errors import CheckpointError


@dataclass
class _FakeCheckpointStore:
    """A minimal `CheckpointStore` stand-in recording `save` calls in order."""

    order: list[tuple[str, int]] = field(default_factory=list)

    async def load(self) -> int | None:
        return None

    async def save(self, lsn: int, *, connection: object | None = None) -> None:
        self.order.append(("store", lsn))


def _options() -> WalboxOptions:
    return WalboxOptions(
        consumer_name="test-consumer",
        dsn="postgresql://example",
        slot_name="test_slot",
        publication_name="test_pub",
    )


def _client() -> Client:
    return Client(_options(), _FakeCheckpointStore())


def test_record_durable_progress_advances_durable_lsn():
    client = _client()

    client._record_durable_progress(100, 0.01)

    assert client._durable_lsn == 100


def test_record_durable_progress_never_regresses():
    client = _client()

    client._record_durable_progress(100, 0.01)
    client._record_durable_progress(50, 0.01)

    assert client._durable_lsn == 100


def test_record_durable_progress_tracks_the_latest_checkpoint_latency():
    client = _client()

    client._record_durable_progress(100, 0.25)

    assert client._last_checkpoint_latency == 0.25


async def test_checkpoint_handle_save_invokes_the_hook_after_the_store_call():
    store = _FakeCheckpointStore()
    handle = CheckpointHandle(
        store,
        lambda lsn, latency: store.order.append(("hook", lsn)),
    )

    await handle.save(42)

    assert store.order == [("store", 42), ("hook", 42)]


async def test_checkpoint_handle_save_reports_a_nonnegative_latency():
    store = _FakeCheckpointStore()
    latencies = []
    handle = CheckpointHandle(store, lambda lsn, latency: latencies.append(latency))

    await handle.save(42)

    assert len(latencies) == 1
    assert latencies[0] >= 0


async def test_checkpoint_handle_with_no_hook_does_not_raise():
    store = _FakeCheckpointStore()
    handle = CheckpointHandle(store)

    await handle.save(42)

    assert store.order == [("store", 42)]


async def test_checkpoint_handle_save_raises_when_lsn_exceeds_max_lsn():
    store = _FakeCheckpointStore()
    handle = CheckpointHandle(store, None, 50)

    with pytest.raises(CheckpointError):
        await handle.save(51)

    assert store.order == []


async def test_checkpoint_handle_save_allows_lsn_at_the_max():
    store = _FakeCheckpointStore()
    handle = CheckpointHandle(store, None, 50)

    await handle.save(50)

    assert store.order == [("store", 50)]


async def test_send_status_update_uses_durable_lsn_not_last_written_lsn():
    client = _client()
    client._transport = AsyncMock()
    client._last_written_lsn = 500
    client._durable_lsn = 200

    await client._send_status_update(reply_requested=False)

    client._transport.write.assert_awaited_once()
    (payload,), _kwargs = client._transport.write.await_args
    written = int.from_bytes(payload[1:9], "big") - 1
    flushed = int.from_bytes(payload[9:17], "big") - 1
    applied = int.from_bytes(payload[17:25], "big") - 1
    assert written == 500
    assert flushed == 200
    assert applied == 200
