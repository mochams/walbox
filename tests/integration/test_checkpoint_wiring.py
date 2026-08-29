"""Integration test for checkpoint-handle wiring against a real client run.

Exercises `PostgresCheckpointStore`: the application, not the client, is
responsible for calling `checkpoint.save(...)`, exactly as the project's
README example shows -- walbox has no auto-checkpoint mode.
"""

import asyncio
import contextlib
import uuid
from dataclasses import dataclass
from dataclasses import field
from typing import Any

import pytest
from psycopg import AsyncConnection

from walbox.abc import CheckpointHandle
from walbox.abc import Transaction
from walbox.abc import WalboxOptions
from walbox.checkpoint import PostgresCheckpointStore
from walbox.client import Client

pytestmark = pytest.mark.postgres


def _unique_slot_name() -> str:
    return f"slot_{uuid.uuid4().hex}"


def _unique_consumer_name() -> str:
    return f"consumer_{uuid.uuid4().hex}"


@dataclass
class _CountingCheckpointStore:
    """Wraps a real `PostgresCheckpointStore`, counting `save` calls."""

    inner: PostgresCheckpointStore
    save_count: int = field(default=0)

    async def load(self) -> int | None:
        return await self.inner.load()

    async def save(
        self,
        lsn: int,
        *,
        connection: AsyncConnection[Any] | None = None,
    ) -> None:
        self.save_count += 1
        await self.inner.save(lsn, connection=connection)


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


async def test_handler_checkpoints_explicitly_via_the_handle_it_is_given(
    postgres_dsn: str,
    outbox_table: None,
) -> None:
    consumer_name = _unique_consumer_name()
    checkpoint_store = _CountingCheckpointStore(
        PostgresCheckpointStore(postgres_dsn, consumer_name=consumer_name),
    )
    slot_name = _unique_slot_name()
    options = WalboxOptions(
        consumer_name=consumer_name,
        dsn=postgres_dsn,
        slot_name=slot_name,
        publication_name="walbox_pub",
    )
    client = Client(options, checkpoint_store=checkpoint_store)
    saved: list[int] = []

    async def handler(transaction: Transaction, checkpoint: CheckpointHandle) -> None:
        await checkpoint.save(transaction.commit_lsn)
        saved.append(transaction.commit_lsn)

    task = asyncio.ensure_future(client.run(handler))
    try:
        await _wait_slot_active(postgres_dsn, slot_name)

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(
                "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
                "VALUES ('user', 'user-1', 'user_created', '{}'::jsonb)",
            )

        async def _poll() -> None:
            while not saved:
                await asyncio.sleep(0.05)

        await asyncio.wait_for(_poll(), timeout=5.0)

        assert not task.done()
        assert await checkpoint_store.inner.load() == saved[0]
        assert checkpoint_store.save_count == 1
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        if client._transport is not None:
            client._transport.close()
