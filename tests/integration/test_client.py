"""Integration tests for `Client.run` against a real Postgres.

Exercises the guide's own "smallest vertical slice": connect, decode the
byte stream, assemble transactions, and dispatch them to a handler --
including staying alive across a keepalive round trip.
"""

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from dataclasses import field
from unittest.mock import AsyncMock

import pytest
from psycopg import AsyncConnection

from walbox.abc import ChangeKind
from walbox.abc import CheckpointHandle
from walbox.abc import Transaction
from walbox.abc import WalboxOptions
from walbox.client import Client

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

    async def __call__(
        self,
        transaction: Transaction,
        checkpoint: CheckpointHandle,
    ) -> None:
        self.transactions.append(transaction)


def _options(postgres_dsn: str, slot_name: str) -> WalboxOptions:
    return WalboxOptions(
        consumer_name="test-consumer",
        dsn=postgres_dsn,
        slot_name=slot_name,
        publication_name="walbox_pub",
    )


def _client(postgres_dsn: str, slot_name: str) -> Client:
    return Client(_options(postgres_dsn, slot_name), _FakeCheckpointStore())


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
    """Runs a `Client` as a background task for one test's lifetime."""

    def __init__(self, client: Client, handler: _RecordingHandler) -> None:
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
    client = _client(postgres_dsn, slot_name)
    running = _RunningClient(client, handler)
    await _wait_slot_active(postgres_dsn, slot_name)
    return running


async def test_basic_replication_delivers_a_single_insert(postgres_dsn, outbox_table):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    running = await _start_client(postgres_dsn, slot_name, handler)
    try:
        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(
                "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
                "VALUES ('user', 'user-1', 'user_created', '{\"a\": 1}'::jsonb)",
            )

        await _wait_for_count(handler, 1)
        running.raise_if_failed()

        transaction = handler.transactions[0]
        assert len(transaction.changes) == 1
        change = transaction.changes[0]
        assert change.kind == ChangeKind.INSERT
        assert change.table == "public.outbox"
        assert change.new["entity_type"] == "user"
        assert change.new["entity_id"] == "user-1"
        assert change.new["event_type"] == "user_created"
    finally:
        await running.stop()


async def test_multiple_inserts_in_one_transaction_arrive_as_one_transaction(
    postgres_dsn,
    outbox_table,
):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    running = await _start_client(postgres_dsn, slot_name, handler)
    try:
        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute("BEGIN")
            await conn.execute(
                "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
                "VALUES ('user', 'user-1', 'user_created', '{}'::jsonb)",
            )
            await conn.execute(
                "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
                "VALUES ('user', 'user-2', 'user_created', '{}'::jsonb)",
            )
            await conn.execute("COMMIT")

        await _wait_for_count(handler, 1)
        running.raise_if_failed()

        transaction = handler.transactions[0]
        assert len(transaction.changes) == 2
        assert [c.new["entity_id"] for c in transaction.changes] == ["user-1", "user-2"]
    finally:
        await running.stop()


async def test_rollback_delivers_nothing(postgres_dsn, outbox_table):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    running = await _start_client(postgres_dsn, slot_name, handler)
    try:
        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute("BEGIN")
            await conn.execute(
                "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
                "VALUES ('user', 'user-1', 'user_created', '{}'::jsonb)",
            )
            await conn.execute("ROLLBACK")

        await asyncio.sleep(2)
        running.raise_if_failed()
        assert handler.transactions == []
    finally:
        await running.stop()


async def test_transactions_are_delivered_in_commit_order(postgres_dsn, outbox_table):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    running = await _start_client(postgres_dsn, slot_name, handler)
    try:
        conn_a = await AsyncConnection.connect(postgres_dsn, autocommit=True)
        conn_b = await AsyncConnection.connect(postgres_dsn, autocommit=True)
        try:
            await conn_a.execute("BEGIN")
            await conn_b.execute("BEGIN")

            await conn_b.execute(
                "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
                "VALUES ('user', 'committed-first', 'user_created', '{}'::jsonb)",
            )
            await conn_b.execute("COMMIT")

            await conn_a.execute(
                "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
                "VALUES ('user', 'committed-second', 'user_created', '{}'::jsonb)",
            )
            await conn_a.execute("COMMIT")
        finally:
            await conn_a.close()
            await conn_b.close()

        await _wait_for_count(handler, 2)
        running.raise_if_failed()

        entity_ids = [tx.changes[0].new["entity_id"] for tx in handler.transactions]
        assert entity_ids == ["committed-first", "committed-second"]
    finally:
        await running.stop()


@pytest.mark.timeout(30)
async def test_run_survives_a_keepalive_round_trip_without_erroring(
    postgres_dsn,
    outbox_table,
):
    async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as admin:
        await admin.execute("ALTER SYSTEM SET wal_sender_timeout = '1s'")
        await admin.execute("SELECT pg_reload_conf()")
    try:
        slot_name = _unique_slot_name()
        handler = _RecordingHandler()
        client = _client(postgres_dsn, slot_name)

        write_calls: list[bytes] = []
        original_new_transport = client._new_transport

        def _spying_new_transport():
            transport = original_new_transport()
            original_write = transport.write

            async def _spying_write(payload: bytes) -> None:
                write_calls.append(payload)
                await original_write(payload)

            transport.write = AsyncMock(side_effect=_spying_write)
            return transport

        client._new_transport = _spying_new_transport

        task = asyncio.ensure_future(client.run(handler))
        try:
            await _wait_slot_active(postgres_dsn, slot_name)
            await asyncio.sleep(3)
            assert not task.done()
            assert write_calls
        finally:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            if client._transport is not None:
                client._transport.close()
    finally:
        async with await AsyncConnection.connect(
            postgres_dsn,
            autocommit=True,
        ) as admin:
            await admin.execute("ALTER SYSTEM RESET wal_sender_timeout")
            await admin.execute("SELECT pg_reload_conf()")
