"""Integration tests for reconnect/resume against a real Postgres.

Uses `pg_terminate_backend` (the same technique `tests/integration/test_transport.py`
uses) to simulate an abrupt disconnection, and asserts `WalboxClient.run`
reconnects and always resumes from the durable checkpoint -- redelivering a
transaction that was never checkpointed, but never one that was.
"""

import asyncio
import contextlib
import uuid
from dataclasses import dataclass
from dataclasses import field

import pytest
from psycopg import AsyncConnection

from walbox.abc import CheckpointHandle
from walbox.abc import Transaction
from walbox.abc import WalboxOptions
from walbox.checkpoint import PostgresCheckpointStore
from walbox.client import WalboxClient

pytestmark = pytest.mark.postgres


def _unique_slot_name() -> str:
    return f"slot_{uuid.uuid4().hex}"


def _unique_consumer_name() -> str:
    return f"consumer_{uuid.uuid4().hex}"


def _insert_row(entity_id: str) -> str:
    return (
        "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
        f"VALUES ('user', '{entity_id}', 'user_created', '{{}}'::jsonb)"
    )


@dataclass
class _RecordingHandler:
    """Collects delivered `Transaction`s in the order the handler saw them,
    checkpointing each one immediately after recording it.
    """

    transactions: list[Transaction] = field(default_factory=list)

    async def __call__(
        self,
        transaction: Transaction,
        checkpoint: CheckpointHandle,
    ) -> None:
        self.transactions.append(transaction)
        await checkpoint.save(transaction.commit_lsn)


def _options(
    postgres_dsn: str,
    slot_name: str,
    consumer_name: str,
) -> WalboxOptions:
    return WalboxOptions(
        consumer_name=consumer_name,
        dsn=postgres_dsn,
        slot_name=slot_name,
        publication_name="walbox_pub",
    )


def _client(postgres_dsn: str, slot_name: str, consumer_name: str) -> WalboxClient:
    return WalboxClient(
        _options(postgres_dsn, slot_name, consumer_name),
        PostgresCheckpointStore(postgres_dsn, consumer_name=consumer_name),
    )


async def _wait_for_count(
    transactions: list[Transaction],
    count: int,
    timeout: float = 15.0,
) -> None:
    async def _poll() -> None:
        while len(transactions) < count:
            await asyncio.sleep(0.05)

    await asyncio.wait_for(_poll(), timeout=timeout)


async def _wait_slot_active(dsn: str, slot_name: str, attempts: int = 150) -> None:
    async with await AsyncConnection.connect(dsn, autocommit=True) as conn:
        for _ in range(attempts):
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT active FROM pg_replication_slots WHERE slot_name = %s",
                    (slot_name,),
                )
                row = await cur.fetchone()
            if row is not None and row[0]:
                return
            await asyncio.sleep(0.1)
    pytest.fail(f"slot {slot_name} did not become active in time")


async def _terminate_backend(dsn: str, backend_pid: int) -> None:
    async with await AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute("SELECT pg_terminate_backend(%s)", (backend_pid,))


class _RunningClient:
    """Runs a `WalboxClient` as a background task for one test's lifetime."""

    def __init__(self, client: WalboxClient, handler) -> None:
        self._client = client
        self._task = asyncio.ensure_future(client.run(handler))

    def backend_pid(self) -> int:
        assert self._client._transport is not None
        return self._client._transport._conn.pgconn.backend_pid

    def raise_if_failed(self) -> None:
        if self._task.done() and not self._task.cancelled():
            exc = self._task.exception()
            if exc is not None:
                raise exc

    async def stop(self) -> None:
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        if self._client._transport is not None:
            self._client._transport.close()


@pytest.mark.timeout(30)
async def test_reconnect_after_a_dropped_connection_keeps_processing(
    postgres_dsn,
    outbox_table,
):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    client = _client(postgres_dsn, slot_name, _unique_consumer_name())
    running = _RunningClient(client, handler)
    try:
        await _wait_slot_active(postgres_dsn, slot_name)

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(_insert_row("tx-a"))
        await _wait_for_count(handler.transactions, 1)
        running.raise_if_failed()

        await _terminate_backend(postgres_dsn, running.backend_pid())

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(_insert_row("tx-b"))
        await _wait_for_count(handler.transactions, 2)
        running.raise_if_failed()

        entity_ids = [tx.changes[0].new["entity_id"] for tx in handler.transactions]
        assert entity_ids == ["tx-a", "tx-b"]
    finally:
        await running.stop()


@pytest.mark.timeout(30)
async def test_crash_before_checkpoint_redelivers_the_same_transaction(
    postgres_dsn,
    outbox_table,
):
    slot_name = _unique_slot_name()
    deliveries: list[Transaction] = []

    async def handler(transaction: Transaction, checkpoint: CheckpointHandle) -> None:
        if len(deliveries) == 1:
            # Second delivery (the redelivery): save before recording it, so
            # a test waiting on `deliveries` reaching length 2 never
            # observes this delivery before the checkpoint is durable.
            await checkpoint.save(transaction.commit_lsn)
        deliveries.append(transaction)

    client = _client(postgres_dsn, slot_name, _unique_consumer_name())
    running = _RunningClient(client, handler)
    try:
        await _wait_slot_active(postgres_dsn, slot_name)

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(_insert_row("crash-redelivery"))
        await _wait_for_count(deliveries, 1)
        running.raise_if_failed()

        await _terminate_backend(postgres_dsn, running.backend_pid())

        await _wait_for_count(deliveries, 2)
        running.raise_if_failed()
        assert deliveries[0].xid == deliveries[1].xid
        assert deliveries[0].commit_lsn == deliveries[1].commit_lsn
        assert [c.new for c in deliveries[0].changes] == [
            c.new for c in deliveries[1].changes
        ]

        await _wait_slot_active(postgres_dsn, slot_name)
        await _terminate_backend(postgres_dsn, running.backend_pid())

        with contextlib.suppress(TimeoutError):
            await _wait_for_count(deliveries, 3, timeout=5.0)
        running.raise_if_failed()
        assert len(deliveries) == 2
    finally:
        await running.stop()


@pytest.mark.timeout(30)
async def test_reconnect_does_not_skip_a_transaction_committed_just_before_the_drop(
    postgres_dsn,
    outbox_table,
):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    client = _client(postgres_dsn, slot_name, _unique_consumer_name())
    running = _RunningClient(client, handler)
    try:
        await _wait_slot_active(postgres_dsn, slot_name)

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(_insert_row("checkpointed-before-drop"))
        await _wait_for_count(handler.transactions, 1)
        running.raise_if_failed()

        await _terminate_backend(postgres_dsn, running.backend_pid())

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(_insert_row("after-reconnect"))
        await _wait_for_count(handler.transactions, 2)
        running.raise_if_failed()

        entity_ids = [tx.changes[0].new["entity_id"] for tx in handler.transactions]
        assert entity_ids == ["checkpointed-before-drop", "after-reconnect"]
    finally:
        await running.stop()
