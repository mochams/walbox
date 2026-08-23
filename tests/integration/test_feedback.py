"""Integration tests for replication feedback against a real Postgres.

Exercises the periodic status-update timer and the durable-progress hook
together: the flush/applied position reported to PostgreSQL must track the
actual durable checkpoint -- advanced only by the handler calling
`checkpoint.save(...)` itself, walbox has no auto-checkpoint mode -- and a
handler that never checkpoints must never advance the reported floor.
"""

import asyncio
import contextlib
import uuid
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from psycopg import AsyncConnection

from walbox.abc import CheckpointHandle
from walbox.abc import ReplicationOptions
from walbox.abc import Transaction
from walbox.checkpoint import FileCheckpointStore
from walbox.client import ReplicationClient

pytestmark = pytest.mark.postgres

_STATUS_INTERVAL = 1


def _unique_slot_name() -> str:
    return f"slot_{uuid.uuid4().hex}"


@dataclass
class _NoOpCheckpointStore:
    """Reports no prior checkpoint and never durably persists anything."""

    async def load(self) -> int | None:
        return None

    async def save(self, lsn: int, *, connection: object | None = None) -> None:
        raise NotImplementedError


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


def _decode_flushed_lsn(payload: bytes) -> int:
    return int.from_bytes(payload[9:17], "big") - 1


class _RunningClient:
    """Runs a `ReplicationClient` as a background task for one test's lifetime."""

    def __init__(self, client: ReplicationClient, handler) -> None:
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


@pytest.mark.timeout(30)
async def test_periodic_status_update_is_sent_without_any_transaction_activity(
    postgres_dsn, outbox_table
):
    slot_name = _unique_slot_name()
    options = ReplicationOptions(
        consumer_name="test-consumer",
        dsn=postgres_dsn,
        slot_name=slot_name,
        publication_name="walbox_pub",
        checkpoint_store=_NoOpCheckpointStore(),
        status_interval=_STATUS_INTERVAL,
    )
    client = ReplicationClient(options)

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
    running = _RunningClient(client, AsyncMock())
    try:
        await _wait_slot_active(postgres_dsn, slot_name)
        await asyncio.sleep(_STATUS_INTERVAL + 1)
        running.raise_if_failed()
        assert write_calls
    finally:
        await running.stop()


@pytest.mark.timeout(30)
async def test_feedback_reflects_the_checkpoint_after_a_manual_save(
    postgres_dsn, outbox_table, tmp_path: Path
):
    checkpoint_store = FileCheckpointStore(tmp_path / "checkpoint")
    slot_name = _unique_slot_name()
    options = ReplicationOptions(
        consumer_name="test-consumer",
        dsn=postgres_dsn,
        slot_name=slot_name,
        publication_name="walbox_pub",
        checkpoint_store=checkpoint_store,
        status_interval=_STATUS_INTERVAL,
    )
    client = ReplicationClient(options)
    saved: list[int] = []

    async def handler(transaction: Transaction, checkpoint: CheckpointHandle) -> None:
        await checkpoint.save(transaction.commit_lsn)
        saved.append(transaction.commit_lsn)

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
    running = _RunningClient(client, handler)
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

        write_calls.clear()
        await asyncio.sleep(_STATUS_INTERVAL + 1)
        running.raise_if_failed()
        assert write_calls
        assert _decode_flushed_lsn(write_calls[-1]) == saved[0]
    finally:
        await running.stop()


@pytest.mark.timeout(30)
async def test_feedback_stays_at_the_floor_when_the_handler_never_saves(
    postgres_dsn, outbox_table, tmp_path: Path
):
    checkpoint_store = FileCheckpointStore(tmp_path / "checkpoint")
    slot_name = _unique_slot_name()
    options = ReplicationOptions(
        consumer_name="test-consumer",
        dsn=postgres_dsn,
        slot_name=slot_name,
        publication_name="walbox_pub",
        checkpoint_store=checkpoint_store,
        status_interval=_STATUS_INTERVAL,
    )
    client = ReplicationClient(options)
    seen: list[Transaction] = []

    async def handler(transaction: Transaction, checkpoint: CheckpointHandle) -> None:
        seen.append(transaction)

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
    running = _RunningClient(client, handler)
    try:
        await _wait_slot_active(postgres_dsn, slot_name)

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(
                "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
                "VALUES ('user', 'user-1', 'user_created', '{}'::jsonb)",
            )

        async def _poll() -> None:
            while not seen:
                await asyncio.sleep(0.05)

        await asyncio.wait_for(_poll(), timeout=5.0)

        write_calls.clear()
        await asyncio.sleep((_STATUS_INTERVAL * 3) + 1)
        running.raise_if_failed()
        assert write_calls
        assert all(_decode_flushed_lsn(payload) == 0 for payload in write_calls)
    finally:
        await running.stop()
