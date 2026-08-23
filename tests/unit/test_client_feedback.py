"""Unit tests for replication feedback: the durable-progress hook and the
`_durable_lsn`-derived status updates it feeds.

Pure, no Postgres: a fake `CheckpointStore` double and a stub `_transport`,
no real filesystem or network I/O.
"""

from dataclasses import dataclass
from dataclasses import field
from unittest.mock import AsyncMock

from walbox.abc import CheckpointHandle
from walbox.abc import ReplicationOptions
from walbox.client import ReplicationClient


@dataclass
class _FakeCheckpointStore:
    """A minimal `CheckpointStore` stand-in recording `save` calls in order."""

    order: list[tuple[str, int]] = field(default_factory=list)

    async def load(self) -> int | None:
        return None

    async def save(self, lsn: int, *, connection: object | None = None) -> None:
        self.order.append(("store", lsn))


def _options() -> ReplicationOptions:
    return ReplicationOptions(
        consumer_name="test-consumer",
        dsn="postgresql://example",
        slot_name="test_slot",
        publication_name="test_pub",
        checkpoint_store=_FakeCheckpointStore(),
    )


def test_record_durable_progress_advances_durable_lsn():
    client = ReplicationClient(_options())

    client._record_durable_progress(100)

    assert client._durable_lsn == 100


def test_record_durable_progress_never_regresses():
    client = ReplicationClient(_options())

    client._record_durable_progress(100)
    client._record_durable_progress(50)

    assert client._durable_lsn == 100


async def test_checkpoint_handle_save_invokes_the_hook_after_the_store_call():
    store = _FakeCheckpointStore()
    handle = CheckpointHandle(store, lambda lsn: store.order.append(("hook", lsn)))

    await handle.save(42)

    assert store.order == [("store", 42), ("hook", 42)]


async def test_checkpoint_handle_with_no_hook_does_not_raise():
    store = _FakeCheckpointStore()
    handle = CheckpointHandle(store)

    await handle.save(42)

    assert store.order == [("store", 42)]


async def test_send_status_update_uses_durable_lsn_not_last_written_lsn():
    client = ReplicationClient(_options())
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
