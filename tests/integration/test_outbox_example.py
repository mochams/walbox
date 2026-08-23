"""Proves examples/outbox.py's quickstart is the same code that's tested.

Both `handle` and `handle_with_atomic_checkpoint` are imported directly from the
example (never reimplemented here) and run against a real Postgres -- `handle`
with `publish_to_broker` swapped for a recording double, `handle_with_atomic_checkpoint`
by checking what it actually persisted -- so the documented quickstart and the
exactly-once-effects pattern in README.md cannot silently drift from working code.
"""

import asyncio
import contextlib
import uuid
from dataclasses import dataclass
from dataclasses import field
from typing import Any

import pytest
from psycopg import AsyncConnection

from examples import outbox
from walbox import PostgresCheckpointStore
from walbox import ReplicationClient
from walbox import ReplicationOptions
from walbox import Transaction
from walbox.abc import CheckpointHandle

pytestmark = pytest.mark.postgres


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


@dataclass
class _RecordingPublisher:
    """Stands in for `publish_to_broker`, recording every published payload."""

    payloads: list[dict[str, Any]] = field(default_factory=list)

    async def __call__(self, payload: dict[str, Any]) -> None:
        self.payloads.append(payload)


async def _wait_for_count(
    publisher: _RecordingPublisher,
    count: int,
    timeout: float = 5.0,
) -> None:
    async def _poll() -> None:
        while len(publisher.payloads) < count:
            await asyncio.sleep(0.05)

    await asyncio.wait_for(_poll(), timeout=timeout)


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


async def test_outbox_example_handle_publishes_and_checkpoints_an_insert(
    postgres_dsn, outbox_table, monkeypatch
) -> None:
    publisher = _RecordingPublisher()
    monkeypatch.setattr(outbox, "publish_to_broker", publisher)

    slot_name = _unique_slot_name()
    checkpoint_store = _FakeCheckpointStore()
    options = ReplicationOptions(
        consumer_name="test-outbox-consumer",
        dsn=postgres_dsn,
        slot_name=slot_name,
        publication_name="walbox_pub",
        checkpoint_store=checkpoint_store,
        manage_checkpoint=False,
    )
    client = ReplicationClient(options)
    task = asyncio.ensure_future(client.run(outbox.handle))
    try:
        await _wait_slot_active(postgres_dsn, slot_name)

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(
                "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
                "VALUES ('user', 'user-1', 'user_created', '{\"a\": 1}'::jsonb)",
            )

        await _wait_for_count(publisher, 1)
        assert not task.done()

        payload = publisher.payloads[0]
        assert payload["entity_type"] == "user"
        assert payload["entity_id"] == "user-1"
        assert payload["event_type"] == "user_created"

        # handle() checkpoints explicitly, after publishing succeeds.
        assert checkpoint_store.checkpoint_lsn is not None
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        if client._transport is not None:
            client._transport.close()


async def test_handle_with_atomic_checkpoint_persists_lsn_via_caller_connection(
    postgres_dsn,
) -> None:
    """Proves `examples/outbox.py`'s atomic-checkpoint handler actually persists.

    `PostgresCheckpointStore.save(connection=conn)` only upserts on the given
    connection -- it never commits. `handle_with_atomic_checkpoint`
    is only correct if its own `conn.commit()` is what makes that durable. A
    *second*, independent store instance (same consumer, fresh connection) is
    used to load the result back, so this cannot pass by reading in-memory
    state that was never actually written to Postgres.
    """
    consumer_name = _unique_consumer_name()
    store = PostgresCheckpointStore(postgres_dsn, consumer_name=consumer_name)
    await store.load()  # mimics ReplicationClient.run()'s startup load, which
    # creates PostgresCheckpointStore's backing table before any handler runs.

    tx = Transaction(
        xid=1,
        commit_lsn=123456,
        commit_time=0,
        checkpoint=CheckpointHandle(store),
    )
    await outbox.handle_with_atomic_checkpoint(tx, postgres_dsn)

    reloaded = PostgresCheckpointStore(postgres_dsn, consumer_name=consumer_name)
    assert await reloaded.load() == 123456
