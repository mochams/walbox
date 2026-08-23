"""Unit tests for checkpoint-handle wiring in `client.py`.

Pure, no Postgres: a fake `CheckpointStore` double, with a synthetic
Begin/Commit pair driven through `_handle_xlog_data` (enqueue) and then
`_process` (checkpoint/handler wiring, the consumer-side split of what
used to be one step), no real filesystem or network I/O.
"""

from dataclasses import dataclass
from dataclasses import field
from unittest.mock import AsyncMock

from walbox.abc import CheckpointHandle
from walbox.abc import ReplicationOptions
from walbox.abc import Transaction
from walbox.client import Handler
from walbox.client import ReplicationClient
from walbox.protocol import XLogData


@dataclass
class _RecordingCheckpointStore:
    """A `CheckpointStore` double recording every `save` call, in order."""

    order: list[str] = field(default_factory=list)

    async def load(self) -> int | None:
        return None

    async def save(self, lsn: int, *, connection: object | None = None) -> None:
        self.order.append(f"save:{lsn}")


def _options(*, checkpoint_store: _RecordingCheckpointStore) -> ReplicationOptions:
    return ReplicationOptions(
        consumer_name="test-consumer",
        dsn="postgresql://example",
        slot_name="test_slot",
        publication_name="test_pub",
        checkpoint_store=checkpoint_store,
    )


def _begin_payload(final_lsn: int = 100, commit_time: int = 111, xid: int = 1) -> bytes:
    return (
        b"B"
        + final_lsn.to_bytes(8, "big")
        + commit_time.to_bytes(8, "big")
        + xid.to_bytes(4, "big")
    )


def _commit_payload(
    commit_lsn: int = 100,
    end_lsn: int = 150,
    commit_time: int = 222,
    flags: int = 0,
) -> bytes:
    return (
        b"C"
        + flags.to_bytes(1, "big")
        + commit_lsn.to_bytes(8, "big")
        + end_lsn.to_bytes(8, "big")
        + commit_time.to_bytes(8, "big")
    )


async def _feed_one_transaction(client: ReplicationClient, handler: Handler) -> None:
    """Assemble a synthetic Begin/Commit sequence and process the result.

    `_handle_xlog_data` only decodes and enqueues; the checkpoint/handler
    wiring under test here now lives in `_process`, so this drains the one
    transaction `_handle_xlog_data` queued and runs it through `_process`
    directly.
    """
    begin_xlog = XLogData(wal_start=1, wal_end=1, send_time=0, payload=_begin_payload())
    await client._handle_xlog_data(begin_xlog)
    commit_xlog = XLogData(
        wal_start=2, wal_end=2, send_time=0, payload=_commit_payload()
    )
    await client._handle_xlog_data(commit_xlog)
    transaction = client._queue.get_nowait()
    await client._process(transaction, handler)


async def test_handler_receives_a_checkpoint_handle_as_its_second_argument():
    store = _RecordingCheckpointStore()
    client = ReplicationClient(_options(checkpoint_store=store))
    seen: list[tuple[Transaction, CheckpointHandle]] = []

    async def handler(transaction: Transaction, checkpoint: CheckpointHandle) -> None:
        seen.append((transaction, checkpoint))

    await _feed_one_transaction(client, handler)

    assert len(seen) == 1
    _transaction, checkpoint = seen[0]
    assert isinstance(checkpoint, CheckpointHandle)


async def test_client_never_calls_save_automatically():
    """The handler is always solely responsible for calling `checkpoint.save`.

    walbox has no auto-checkpoint mode: a handler that never calls
    `checkpoint.save(...)` itself leaves the store untouched, no matter how
    many transactions are processed.
    """
    store = _RecordingCheckpointStore()
    client = ReplicationClient(_options(checkpoint_store=store))
    handler = AsyncMock()

    await _feed_one_transaction(client, handler)

    assert store.order == []


async def test_handler_calling_checkpoint_save_is_what_persists_progress():
    store = _RecordingCheckpointStore()
    client = ReplicationClient(_options(checkpoint_store=store))

    async def handler(transaction: Transaction, checkpoint: CheckpointHandle) -> None:
        await checkpoint.save(transaction.commit_lsn)

    await _feed_one_transaction(client, handler)

    # commit_lsn is derived from end_lsn - 1 (see TransactionAssembler): 150 - 1 == 149
    assert store.order == ["save:149"]
