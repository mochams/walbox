"""Integration test for structured log context.

Runs one transaction through a real Postgres fixture and asserts that a
log record produced along the way carries that transaction's xid and
commit LSN in `extra`, so anyone grepping/aggregating logs and raised
errors can correlate them.
"""

import asyncio
import contextlib
import logging
import uuid
from dataclasses import dataclass
from dataclasses import field

import pytest
from psycopg import AsyncConnection

from walbox.abc import CheckpointHandle
from walbox.abc import Transaction
from walbox.abc import WalboxOptions
from walbox.client import Client

pytestmark = pytest.mark.postgres


def _unique_slot_name() -> str:
    return f"slot_{uuid.uuid4().hex}"


@dataclass
class _FakeCheckpointStore:
    checkpoint_lsn: int | None = None

    async def load(self) -> int | None:
        return self.checkpoint_lsn

    async def save(self, lsn: int, *, connection: object | None = None) -> None:
        self.checkpoint_lsn = lsn


@dataclass
class _RecordingHandler:
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


async def test_log_records_include_xid_and_lsn_context(
    postgres_dsn,
    outbox_table,
    caplog,
):
    slot_name = _unique_slot_name()
    handler = _RecordingHandler()
    client = _client(postgres_dsn, slot_name)

    task = asyncio.ensure_future(client.run(handler))
    try:
        await _wait_slot_active(postgres_dsn, slot_name)
        with caplog.at_level(logging.DEBUG, logger="walbox.transaction"):
            async with await AsyncConnection.connect(
                postgres_dsn,
                autocommit=True,
            ) as conn:
                await conn.execute(
                    "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
                    "VALUES ('user', 'user-1', 'user_created', '{}'::jsonb)",
                )

            await _wait_for_count(handler, 1)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        if client._transport is not None:
            client._transport.close()

    transaction = handler.transactions[0]
    matching = [
        record
        for record in caplog.records
        if getattr(record, "xid", None) == transaction.xid
        and getattr(record, "lsn", None) == transaction.commit_lsn
    ]
    assert matching, (
        "expected a log record carrying the transaction's xid and commit lsn"
    )
