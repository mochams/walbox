"""Unit tests for the metrics hook: `Metrics` and its wiring.

Pure, no Postgres: transactions are handed directly to `_process` rather
than assembled from a real wire stream, and `_handle_xlog_data`/
`_handle_keepalive` are driven with synthetic `XLogData`/`PrimaryKeepalive`
values.
"""

import asyncio
import logging
from dataclasses import dataclass
from dataclasses import field
from unittest.mock import AsyncMock

from walbox.abc import ChangeEvent
from walbox.abc import ChangeKind
from walbox.abc import CheckpointHandle
from walbox.abc import Metrics
from walbox.abc import ReplicationOptions
from walbox.abc import Transaction
from walbox.client import ReplicationClient
from walbox.protocol import PrimaryKeepalive
from walbox.protocol import XLogData


@dataclass
class _FakeCheckpointStore:
    """A minimal `CheckpointStore` stand-in, no disk/DB involved."""

    checkpoint_lsn: int | None = None
    saved: list[int] = field(default_factory=list)

    async def load(self) -> int | None:
        return self.checkpoint_lsn

    async def save(self, lsn: int, *, connection: object | None = None) -> None:
        self.saved.append(lsn)


def _options(**kwargs: object) -> ReplicationOptions:
    kwargs.setdefault("checkpoint_store", _FakeCheckpointStore())
    return ReplicationOptions(
        consumer_name="test-consumer",
        dsn="postgresql://example",
        slot_name="test_slot",
        publication_name="test_pub",
        **kwargs,
    )


def _transaction(
    xid: int = 1, commit_lsn: int = 100, n_changes: int = 1
) -> Transaction:
    return Transaction(
        xid=xid,
        commit_lsn=commit_lsn,
        commit_time=0,
        changes=[
            ChangeEvent(kind=ChangeKind.INSERT, table="public.t", new={"id": str(i)})
            for i in range(n_changes)
        ],
    )


def _type_payload(
    type_oid: int = 16400, namespace: str = "public", name: str = "e"
) -> bytes:
    def _cstring(value: str) -> bytes:
        return value.encode("utf-8") + b"\x00"

    return b"Y" + type_oid.to_bytes(4, "big") + _cstring(namespace) + _cstring(name)


async def test_metrics_callback_invoked_with_current_counters():
    recorded: list[Metrics] = []
    client = ReplicationClient(_options(on_metrics=recorded.append))

    async def handler(transaction: Transaction, checkpoint: CheckpointHandle) -> None:
        await checkpoint.save(transaction.commit_lsn)

    await client._process(_transaction(xid=1, commit_lsn=100, n_changes=2), handler)
    await client._process(_transaction(xid=2, commit_lsn=200, n_changes=3), handler)
    await client._maybe_report_metrics()

    assert len(recorded) == 1
    metrics = recorded[0]
    assert metrics.transactions_processed == 2
    assert metrics.changes_processed == 5
    assert metrics.checkpoint_lsn == 200


async def test_checkpoint_latency_reflects_the_handler_calling_save():
    @dataclass
    class _SlowCheckpointStore:
        async def load(self) -> int | None:
            return None

        async def save(self, lsn: int, *, connection: object | None = None) -> None:
            await asyncio.sleep(0.05)

    client = ReplicationClient(_options(checkpoint_store=_SlowCheckpointStore()))
    assert client._current_metrics().last_checkpoint_latency_seconds == 0.0

    async def handler(transaction: Transaction, checkpoint: CheckpointHandle) -> None:
        await checkpoint.save(transaction.commit_lsn)

    await client._process(_transaction(xid=1, commit_lsn=100), handler)

    assert client._current_metrics().last_checkpoint_latency_seconds >= 0.05


async def test_checkpoint_latency_stays_at_zero_if_the_handler_never_saves():
    client = ReplicationClient(_options())
    handler = AsyncMock()

    await client._process(_transaction(xid=1, commit_lsn=100), handler)

    assert client._current_metrics().last_checkpoint_latency_seconds == 0.0


async def test_metrics_callback_exception_is_caught_and_logged(caplog):
    def _raising_callback(metrics: Metrics) -> None:
        raise ValueError(metrics)

    client = ReplicationClient(_options(on_metrics=_raising_callback))

    with caplog.at_level(logging.ERROR, logger="walbox.client"):
        await client._maybe_report_metrics()  # must not raise

    assert any("metrics callback" in record.message for record in caplog.records)


async def test_no_metrics_callback_configured_is_a_no_op():
    client = ReplicationClient(_options())
    assert client.options.on_metrics is None

    await client._maybe_report_metrics()  # must not raise


async def test_replication_lag_reflects_keepalive_wal_end():
    client = ReplicationClient(_options())
    xlog = XLogData(wal_start=700, wal_end=700, send_time=0, payload=_type_payload())
    await client._handle_xlog_data(xlog)

    keepalive = PrimaryKeepalive(wal_end=1000, send_time=0, reply_requested=False)
    await client._handle_keepalive(keepalive)

    metrics = client._current_metrics()
    assert metrics.receive_lsn == 700
    assert metrics.replication_lag_bytes == 300


async def test_queue_depth_reflects_actual_queue_size():
    client = ReplicationClient(_options(max_pending_transactions=10))
    for xid in range(3):
        client._queue.put_nowait(_transaction(xid=xid, commit_lsn=xid))

    metrics = client._current_metrics()
    assert metrics.queue_depth == 3
