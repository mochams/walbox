"""Replication connections through pgbouncer, end to end.

A client that requests `replication=database` (walbox always does, see
`walbox/transport.py`) gets a transparent 1:1 passthrough on pgbouncer
>= 1.23.0 in `session` or `transaction` pool_mode. A replication stream
can't be pooled or multiplexed, so pgbouncer treats it as a dedicated
connection rather than trying to fit it into either mode's usual pooling
behavior. Only these two pool_modes are exercised here: `statement`
pool_mode can't run the checkpoint store at all (see
docs/production/setup.md#pgbouncer for why), so it's excluded from
`conftest.py`'s `pool_mode` fixture rather than tested and marked as
failing in every test that depends on it.

This is verified here by actually creating the slot, starting replication,
inserting a row, and checking walbox's own handler receives it, not just by
checking that the initial connection succeeds. `tests/integration/conftest.py`
also asserts the pgbouncer image in use meets the version this depends on
(`_check_pgbouncer_version`), so a future image swap that drops below it
fails loudly here instead of silently reintroducing the wrong conclusion an
earlier version of this suite drew from an abandoned, years-out-of-date
pgbouncer image.
"""

import asyncio
import contextlib
import uuid
from dataclasses import dataclass
from dataclasses import field
from typing import Protocol

import pytest
from psycopg import AsyncConnection

from walbox.abc import CheckpointHandle
from walbox.abc import Transaction
from walbox.abc import WalboxOptions
from walbox.checkpoint import PostgresCheckpointStore
from walbox.client import Client

pytestmark = pytest.mark.pgbouncer


def _unique_slot_name() -> str:
    return f"slot_{uuid.uuid4().hex}"


def _unique_consumer_name() -> str:
    return f"consumer_{uuid.uuid4().hex}"


@dataclass
class _FakeCheckpointStore:
    """A minimal `CheckpointStore` stand-in reporting no prior checkpoint."""

    checkpoint_lsn: int | None = None

    async def load(self) -> int | None:
        return self.checkpoint_lsn

    async def save(self, lsn: int, *, connection: object | None = None) -> None:
        self.checkpoint_lsn = lsn


class _RecordsTransactions(Protocol):
    """Structural type shared by `_RecordingHandler` and `_CheckpointingHandler`."""

    transactions: list[Transaction]

    async def __call__(
        self,
        transaction: Transaction,
        checkpoint: CheckpointHandle,
    ) -> None: ...


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


@dataclass
class _CheckpointingHandler:
    """Like `_RecordingHandler`, but also checkpoints through the handle.

    Real handlers checkpoint via `checkpoint.save(...)`, not by holding a
    separate reference to the store, so a checkpoint failure surfaces the
    way `Client.run()` actually propagates one: out of its own task, not
    from wherever the handler happened to be called from.
    """

    transactions: list[Transaction] = field(default_factory=list)

    async def __call__(
        self,
        transaction: Transaction,
        checkpoint: CheckpointHandle,
    ) -> None:
        self.transactions.append(transaction)
        await checkpoint.save(transaction.commit_lsn)


async def _wait_slot_active(dsn: str, slot_name: str, attempts: int = 100) -> None:
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


async def _wait_for_count(
    handler: _RecordsTransactions,
    count: int,
    timeout: float = 10.0,
) -> None:
    async def _poll() -> None:
        while len(handler.transactions) < count:
            await asyncio.sleep(0.05)

    await asyncio.wait_for(_poll(), timeout=timeout)


class _RunningClient:
    """Runs a `Client` as a background task for one test's lifetime."""

    def __init__(self, client: Client, handler: _RecordsTransactions) -> None:
        self._client = client
        self._task = asyncio.ensure_future(client.run(handler))

    async def wait(self, timeout: float = 10.0) -> None:
        """Await the client's own task, propagating whatever it raised.

        Unlike `stop()`, this doesn't cancel or swallow anything: it's for
        asserting on a failure the client is expected to surface on its
        own, not for teardown.
        """
        await asyncio.wait_for(self._task, timeout=timeout)

    async def stop(self) -> None:
        """Cancel the client's task and clean up, swallowing whatever it raised.

        Pure teardown: any failure worth asserting on should already have
        been observed via `raise_if_failed()` or `wait()` before this runs,
        so nothing here needs to propagate.
        """
        self._task.cancel()
        with contextlib.suppress(BaseException):
            await self._task
        if self._client._transport is not None:
            self._client._transport.close()

    def raise_if_failed(self) -> None:
        if self._task.done() and not self._task.cancelled():
            exc = self._task.exception()
            if exc is not None:
                raise exc


async def _insert_row(dsn: str, entity_id: str) -> None:
    async with await AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute(
            "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
            "VALUES ('user', %s, 'user_created', '{}'::jsonb)",
            (entity_id,),
        )


@pytest.mark.timeout(60)
async def test_replicates_and_delivers_a_row_through_pgbouncer(
    pgbouncer_dsn: str,
    pgbouncer_outbox_table: None,
    pgbouncer_postgres_dsn: str,
    pool_mode: str,
) -> None:
    slot_name = _unique_slot_name()
    options = WalboxOptions(
        consumer_name=_unique_consumer_name(),
        dsn=pgbouncer_dsn,
        slot_name=slot_name,
        publication_name="walbox_pub",
    )
    handler = _RecordingHandler()
    running = _RunningClient(Client(options, _FakeCheckpointStore()), handler)
    try:
        await _wait_slot_active(pgbouncer_postgres_dsn, slot_name)
        await _insert_row(pgbouncer_postgres_dsn, "user-1")

        await _wait_for_count(handler, 1)
        running.raise_if_failed()

        transaction = handler.transactions[0]
        assert len(transaction.changes) == 1
        change = transaction.changes[0]
        assert change.table == "public.outbox"
        assert change.new["entity_id"] == "user-1"
    finally:
        await running.stop()


@pytest.mark.timeout(60)
async def test_one_dsn_covers_replication_and_the_checkpoint_store(
    pgbouncer_dsn: str,
    pgbouncer_outbox_table: None,
    pgbouncer_postgres_dsn: str,
    pool_mode: str,
) -> None:
    """One pgbouncer DSN, used for both the replication stream and checkpoints.

    Earlier guidance assumed the replication connection always needed its
    own direct-to-Postgres DSN, separate from anything pooled. That's only
    true against an old pgbouncer, or `statement` pool_mode. Here,
    `pgbouncer_dsn` is the *only* connection string walbox is given:
    `PostgresCheckpointStore` durably saves the checkpoint through the same
    pgbouncer the replication stream runs through, exactly as a deployment
    with one connection string configured would use it.
    """
    slot_name = _unique_slot_name()
    checkpoint_store = PostgresCheckpointStore(
        pgbouncer_dsn,
        consumer_name=_unique_consumer_name(),
        table=f"walbox_checkpoint_{uuid.uuid4().hex}",
    )
    options = WalboxOptions(
        consumer_name=_unique_consumer_name(),
        dsn=pgbouncer_dsn,
        slot_name=slot_name,
        publication_name="walbox_pub",
    )
    handler = _CheckpointingHandler()
    running = _RunningClient(Client(options, checkpoint_store), handler)
    try:
        await _wait_slot_active(pgbouncer_postgres_dsn, slot_name)
        await _insert_row(pgbouncer_postgres_dsn, "user-2")

        await _wait_for_count(handler, 1)
        running.raise_if_failed()

        transaction = handler.transactions[0]
        assert await checkpoint_store.load() == transaction.commit_lsn
    finally:
        await running.stop()
