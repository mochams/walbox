"""Integration tests for streamed transactions against a real Postgres.

`logical_decoding_work_mem` is lowered to its minimum (64kB) for the
duration of each test, so a transaction of a few hundred KB -- not a
genuinely huge one -- is enough to force PostgreSQL to stream it in chunks
via `StreamStart`/`StreamStop`/`StreamCommit` instead of buffering it whole
and delivering it atomically at commit.
The payload uses concatenated, per-row-distinct MD5 hashes rather than a
repeated character so it doesn't compress away to well under the threshold
before `ReorderBuffer` ever sees it.
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
from walbox.abc import CheckpointHandle
from walbox.abc import Transaction
from walbox.abc import WalboxOptions
from walbox.client import WalboxClient

pytestmark = pytest.mark.postgres

_ROW_COUNT = 200
_HASHES_PER_ROW = 50  # ~50 * 32 hex chars =~ 1.6KB of distinct data per row


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


def _client(postgres_dsn: str, slot_name: str) -> WalboxClient:
    return WalboxClient(_options(postgres_dsn, slot_name), _FakeCheckpointStore())


async def _wait_for_count(
    handler: _RecordingHandler,
    count: int,
    timeout: float = 15.0,
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
    """Runs a `WalboxClient` as a background task for one test's lifetime."""

    def __init__(self, client: WalboxClient, handler: _RecordingHandler) -> None:
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


def _bulk_insert_sql(row_count: int = _ROW_COUNT) -> str:
    """A single `INSERT ... SELECT` large enough to force streaming.

    Each row's payload is `_HASHES_PER_ROW` distinct MD5 hashes concatenated
    together -- unlike a repeated character, this doesn't compress away to
    a fraction of its logical size before PostgreSQL's reorder buffer ever
    accounts for it.
    """
    return (
        "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
        "SELECT 'user', 'user-' || gs, 'user_created', "
        "jsonb_build_object('data', ("
        "  SELECT string_agg(md5(gs::text || i::text), '') "
        f"  FROM generate_series(1, {_HASHES_PER_ROW}) AS i"
        ")) "
        f"FROM generate_series(1, {row_count}) AS gs"
    )


@pytest.fixture
async def small_logical_decoding_work_mem(postgres_dsn: str) -> AsyncIterator[None]:
    """Lowers `logical_decoding_work_mem` to its 64kB minimum for one test.

    Reloadable via `pg_reload_conf()` (PGC_USERSET, same as
    `wal_sender_timeout` in `tests/integration/test_client.py`'s keepalive
    test) -- no server restart needed, and reset unconditionally afterward
    so it doesn't leak into unrelated tests sharing the session-scoped
    container.
    """
    async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as admin:
        await admin.execute("ALTER SYSTEM SET logical_decoding_work_mem = '64kB'")
        await admin.execute("SELECT pg_reload_conf()")
    try:
        yield
    finally:
        async with await AsyncConnection.connect(
            postgres_dsn,
            autocommit=True,
        ) as admin:
            await admin.execute("ALTER SYSTEM RESET logical_decoding_work_mem")
            await admin.execute("SELECT pg_reload_conf()")


@pytest.mark.timeout(30)
async def test_large_transaction_streams_and_is_delivered_once_committed(
    postgres_dsn,
    outbox_table,
    small_logical_decoding_work_mem,
):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    running = await _start_client(postgres_dsn, slot_name, handler)
    try:
        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute("BEGIN")
            await conn.execute(_bulk_insert_sql())
            await conn.execute(
                "UPDATE outbox SET event_type = 'user_renamed' "
                "WHERE entity_id = 'user-1'",
            )
            await conn.execute("DELETE FROM outbox WHERE entity_id = 'user-2'")
            await conn.execute("COMMIT")

        await _wait_for_count(handler, 1)
        running.raise_if_failed()

        transaction = handler.transactions[0]
        assert len(transaction.changes) == _ROW_COUNT + 2
        kinds = [c.kind for c in transaction.changes]
        assert kinds.count(ChangeKind.INSERT) == _ROW_COUNT
        assert kinds.count(ChangeKind.UPDATE) == 1
        assert kinds.count(ChangeKind.DELETE) == 1
        inserted_ids = [
            c.new["entity_id"]
            for c in transaction.changes
            if c.kind == ChangeKind.INSERT
        ]
        assert inserted_ids == [f"user-{i}" for i in range(1, _ROW_COUNT + 1)]
    finally:
        await running.stop()


@pytest.mark.timeout(30)
async def test_large_transaction_rollback_delivers_nothing(
    postgres_dsn,
    outbox_table,
    small_logical_decoding_work_mem,
):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    running = await _start_client(postgres_dsn, slot_name, handler)
    try:
        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute("BEGIN")
            await conn.execute(_bulk_insert_sql())
            await conn.execute("ROLLBACK")

        # Give the (never-committed, but definitely streamed) transaction's
        # chunks a chance to arrive and be buffered before asserting nothing
        # was ever handed to the application.
        await asyncio.sleep(3)
        running.raise_if_failed()
        assert handler.transactions == []
    finally:
        await running.stop()


@pytest.mark.timeout(30)
async def test_large_transaction_savepoint_rollback_excludes_only_that_savepoint(
    postgres_dsn,
    outbox_table,
    small_logical_decoding_work_mem,
):
    """A `ROLLBACK TO SAVEPOINT` inside a streamed transaction discards only
    the changes made under that savepoint -- not the whole transaction, and
    not changes made before or after it (exercising the precise StreamAbort
    handling, verified against a real streaming PostgreSQL connection).
    """
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    running = await _start_client(postgres_dsn, slot_name, handler)
    try:
        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute("BEGIN")
            await conn.execute(_bulk_insert_sql())  # forces streaming to start
            await conn.execute("SAVEPOINT doomed")
            await conn.execute(
                "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
                "VALUES ('user', 'rolled-back', 'user_created', '{}'::jsonb)",
            )
            await conn.execute("ROLLBACK TO SAVEPOINT doomed")
            await conn.execute(
                "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
                "VALUES ('user', 'kept-after-savepoint', 'user_created', '{}'::jsonb)",
            )
            await conn.execute("COMMIT")

        await _wait_for_count(handler, 1)
        running.raise_if_failed()

        transaction = handler.transactions[0]
        entity_ids = [c.new["entity_id"] for c in transaction.changes]
        assert "rolled-back" not in entity_ids
        assert "kept-after-savepoint" in entity_ids
        assert len(transaction.changes) == _ROW_COUNT + 1
    finally:
        await running.stop()


@pytest.mark.timeout(30)
async def test_streaming_does_not_starve_smaller_concurrent_transactions(
    postgres_dsn,
    outbox_table,
    small_logical_decoding_work_mem,
):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    running = await _start_client(postgres_dsn, slot_name, handler)
    big_conn = await AsyncConnection.connect(postgres_dsn, autocommit=True)
    try:
        # Held open across the small transaction below -- its chunks are
        # already streamed to walbox by the time this `execute()` returns
        # (the INSERT's WAL is written well before the explicit COMMIT),
        # well before the big transaction itself ever commits.
        await big_conn.execute("BEGIN")
        await big_conn.execute(_bulk_insert_sql())

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(
                "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
                "VALUES ('user', 'small-tx', 'user_created', '{}'::jsonb)",
            )

        await _wait_for_count(handler, 1, timeout=10)
        running.raise_if_failed()
        small = handler.transactions[0]
        assert len(small.changes) == 1
        assert small.changes[0].new["entity_id"] == "small-tx"

        await big_conn.execute("COMMIT")
        await _wait_for_count(handler, 2, timeout=15)
        running.raise_if_failed()
        big = handler.transactions[1]
        assert len(big.changes) == _ROW_COUNT
    finally:
        await big_conn.close()
        await running.stop()
