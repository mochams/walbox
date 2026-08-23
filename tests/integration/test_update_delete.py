"""Integration tests for Update/Delete decoding against a real Postgres.

Exercises all three REPLICA IDENTITY wire shapes end to end through
`ReplicationClient`: DEFAULT (key-only old tuple on a key change, no old
tuple otherwise), and FULL (complete old row on any change).
"""

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from dataclasses import field

import pytest
from psycopg import AsyncConnection

from walbox.abc import ChangeKind
from walbox.abc import ReplicationOptions
from walbox.abc import Transaction
from walbox.client import ReplicationClient

pytestmark = pytest.mark.postgres


def _unique_slot_name() -> str:
    return f"slot_{uuid.uuid4().hex}"


@dataclass
class _FakeCheckpointStore:
    """A minimal `CheckpointStore` stand-in reporting no prior checkpoint."""

    checkpoint_lsn: int | None = None

    async def load(self) -> int | None:
        return self.checkpoint_lsn

    async def save(self, lsn: int, *, connection: object | None = None) -> None:
        self.checkpoint_lsn = lsn


@dataclass
class _RecordingHandler:
    """Collects delivered `Transaction`s in the order the handler saw them."""

    transactions: list[Transaction] = field(default_factory=list)

    async def __call__(self, transaction: Transaction) -> None:
        self.transactions.append(transaction)


def _options(postgres_dsn: str, slot_name: str) -> ReplicationOptions:
    return ReplicationOptions(
        consumer_name="test-consumer",
        dsn=postgres_dsn,
        slot_name=slot_name,
        publication_name="walbox_pub",
        checkpoint_store=_FakeCheckpointStore(),
    )


async def _wait_for_count(
    handler: _RecordingHandler,
    count: int,
    timeout: float = 5.0,
) -> None:
    async def _poll() -> None:
        while len(handler.transactions) < count:
            await asyncio.sleep(0.05)

    await asyncio.wait_for(_poll(), timeout=timeout)


async def _wait_slot_active(
    dsn: str,
    slot_name: str,
    attempts: int = 100,
) -> None:
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


class _RunningClient:
    """Runs a `ReplicationClient` as a background task for one test's lifetime."""

    def __init__(self, client: ReplicationClient, handler: _RecordingHandler) -> None:
        self._client = client
        self._task = asyncio.ensure_future(client.run(handler))

    async def stop(self) -> None:
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        if self._client._transport is not None:
            self._client._transport.close()

    def raise_if_failed(self) -> None:
        if self._task.done() and not self._task.cancelled():
            exc = self._task.exception()
            if exc is not None:
                raise exc


async def _start_client(
    postgres_dsn: str,
    slot_name: str,
    handler: _RecordingHandler,
) -> AsyncIterator[_RunningClient]:
    client = ReplicationClient(_options(postgres_dsn, slot_name))
    running = _RunningClient(client, handler)
    await _wait_slot_active(postgres_dsn, slot_name)
    return running


async def _insert_row(conn: AsyncConnection, entity_id: str) -> None:
    await conn.execute(
        "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
        "VALUES ('user', %s, 'user_created', '{}'::jsonb)",
        (entity_id,),
    )


async def test_update_default_identity_non_key_column(postgres_dsn, outbox_table):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    running = await _start_client(postgres_dsn, slot_name, handler)
    try:
        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await _insert_row(conn, "user-1")
        await _wait_for_count(handler, 1)

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(
                "UPDATE outbox SET event_type = 'user_renamed' "
                "WHERE entity_id = 'user-1'",
            )
        await _wait_for_count(handler, 2)
        running.raise_if_failed()

        change = handler.transactions[1].changes[0]
        assert change.kind == ChangeKind.UPDATE
        assert change.old is None
        assert change.new["event_type"] == "user_renamed"
        assert change.new["entity_id"] == "user-1"
    finally:
        await running.stop()


async def test_update_default_identity_key_column_changed(postgres_dsn, outbox_table):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    running = await _start_client(postgres_dsn, slot_name, handler)
    try:
        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await _insert_row(conn, "user-1")
        await _wait_for_count(handler, 1)
        inserted_id = handler.transactions[0].changes[0].new["id"]

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(
                "UPDATE outbox SET id = id + 1000000 WHERE entity_id = 'user-1'",
            )
        await _wait_for_count(handler, 2)
        running.raise_if_failed()

        change = handler.transactions[1].changes[0]
        assert change.kind == ChangeKind.UPDATE
        assert change.old == {"id": inserted_id}
        assert change.new["entity_id"] == "user-1"
    finally:
        await running.stop()


async def test_update_full_identity(postgres_dsn, outbox_table):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
        await conn.execute("ALTER TABLE outbox REPLICA IDENTITY FULL")
    running = await _start_client(postgres_dsn, slot_name, handler)
    try:
        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await _insert_row(conn, "user-1")
        await _wait_for_count(handler, 1)

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(
                "UPDATE outbox SET event_type = 'user_renamed' "
                "WHERE entity_id = 'user-1'",
            )
        await _wait_for_count(handler, 2)
        running.raise_if_failed()

        change = handler.transactions[1].changes[0]
        assert change.kind == ChangeKind.UPDATE
        assert change.old is not None
        assert change.old["entity_id"] == "user-1"
        assert change.old["event_type"] == "user_created"
        assert change.new["event_type"] == "user_renamed"
    finally:
        await running.stop()


async def test_delete_default_identity(postgres_dsn, outbox_table):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    running = await _start_client(postgres_dsn, slot_name, handler)
    try:
        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await _insert_row(conn, "user-1")
        await _wait_for_count(handler, 1)
        inserted_id = handler.transactions[0].changes[0].new["id"]

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute("DELETE FROM outbox WHERE entity_id = 'user-1'")
        await _wait_for_count(handler, 2)
        running.raise_if_failed()

        change = handler.transactions[1].changes[0]
        assert change.kind == ChangeKind.DELETE
        assert change.new is None
        assert change.old == {"id": inserted_id}
    finally:
        await running.stop()


async def test_insert_update_delete_in_one_transaction(postgres_dsn, outbox_table):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    running = await _start_client(postgres_dsn, slot_name, handler)
    try:
        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute("BEGIN")
            await _insert_row(conn, "user-1")
            await conn.execute(
                "UPDATE outbox SET event_type = 'user_renamed' "
                "WHERE entity_id = 'user-1'",
            )
            await conn.execute("DELETE FROM outbox WHERE entity_id = 'user-1'")
            await conn.execute("COMMIT")

        await _wait_for_count(handler, 1)
        running.raise_if_failed()

        transaction = handler.transactions[0]
        assert [c.kind for c in transaction.changes] == [
            ChangeKind.INSERT,
            ChangeKind.UPDATE,
            ChangeKind.DELETE,
        ]
    finally:
        await running.stop()
