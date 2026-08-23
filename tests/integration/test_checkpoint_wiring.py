"""Integration test for `manage_checkpoint` wiring against a real client run.

Exercises `FileCheckpointStore` and `manage_checkpoint=False` together: the
application, not the client, is responsible for calling
`tx.checkpoint.save(...)`, exactly as the project's README example shows.
"""

import asyncio
import contextlib
import uuid
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any

import pytest
from psycopg import AsyncConnection

from walbox.abc import ReplicationOptions
from walbox.abc import Transaction
from walbox.checkpoint import FileCheckpointStore
from walbox.client import ReplicationClient

pytestmark = pytest.mark.postgres


def _unique_slot_name() -> str:
    return f"slot_{uuid.uuid4().hex}"


@dataclass
class _CountingCheckpointStore:
    """Wraps a real `FileCheckpointStore`, counting `save` calls."""

    inner: FileCheckpointStore
    save_count: int = field(default=0)

    async def load(self) -> int | None:
        return await self.inner.load()

    async def save(
        self, lsn: int, *, connection: AsyncConnection[Any] | None = None
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


async def test_manage_checkpoint_false_allows_the_handler_to_save_explicitly(
    postgres_dsn: str,
    outbox_table: None,
    tmp_path: Path,
) -> None:
    checkpoint_store = _CountingCheckpointStore(
        FileCheckpointStore(tmp_path / "checkpoint")
    )
    slot_name = _unique_slot_name()
    options = ReplicationOptions(
        consumer_name="test-consumer",
        dsn=postgres_dsn,
        slot_name=slot_name,
        publication_name="walbox_pub",
        checkpoint_store=checkpoint_store,
        manage_checkpoint=False,
    )
    client = ReplicationClient(options)
    saved: list[int] = []

    async def handler(transaction: Transaction) -> None:
        assert transaction.checkpoint is not None
        await transaction.checkpoint.save(transaction.commit_lsn)
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
