"""Integration tests for graceful shutdown against a real Postgres.

Covers four named SIGTERM scenarios: `close()` is what a signal handler
would invoke, and each scenario asserts `ReplicationClient.run` returns
cleanly -- no exception, no cancellation needed -- only once any in-flight
handler has finished and been checkpointed.
"""

import asyncio
import contextlib
import uuid
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


def _insert_row(entity_id: str) -> str:
    return (
        "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
        f"VALUES ('user', '{entity_id}', 'user_created', '{{}}'::jsonb)"
    )


def _entity_id(transaction: Transaction) -> str:
    return transaction.changes[0].new["entity_id"]


def _options(
    postgres_dsn: str,
    slot_name: str,
    checkpoint_path: Path,
    **kwargs: object,
) -> ReplicationOptions:
    return ReplicationOptions(
        consumer_name="test-consumer",
        dsn=postgres_dsn,
        slot_name=slot_name,
        publication_name="walbox_pub",
        checkpoint_store=FileCheckpointStore(checkpoint_path),
        status_interval=_STATUS_INTERVAL,
        **kwargs,
    )


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


async def _wait_for_count(items: list, count: int, timeout: float = 10.0) -> None:
    async def _poll() -> None:
        while len(items) < count:
            await asyncio.sleep(0.05)

    await asyncio.wait_for(_poll(), timeout=timeout)


async def _cleanup(client: ReplicationClient, run_task: "asyncio.Task[None]") -> None:
    """Best-effort teardown, so a failed assertion never leaks a task/connection."""
    if not run_task.done():
        client.close()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(run_task, timeout=5.0)
    if not run_task.done():
        run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task
    if client._transport is not None:
        client._transport.close()


@pytest.mark.timeout(30)
async def test_sigterm_while_idle(postgres_dsn, outbox_table, tmp_path):
    slot_name = _unique_slot_name()
    client = ReplicationClient(
        _options(postgres_dsn, slot_name, tmp_path / "checkpoint")
    )
    run_task = asyncio.ensure_future(client.run(AsyncMock()))
    try:
        await _wait_slot_active(postgres_dsn, slot_name)

        client.close()

        assert await asyncio.wait_for(run_task, timeout=_STATUS_INTERVAL + 5) is None
    finally:
        await _cleanup(client, run_task)


@pytest.mark.timeout(30)
async def test_sigterm_while_receiving(postgres_dsn, outbox_table, tmp_path):
    slot_name = _unique_slot_name()
    client = ReplicationClient(
        _options(postgres_dsn, slot_name, tmp_path / "checkpoint")
    )
    delivered: list[Transaction] = []

    async def handler(transaction: Transaction, checkpoint: CheckpointHandle) -> None:
        delivered.append(transaction)

    run_task = asyncio.ensure_future(client.run(handler))
    committed_ids: list[str] = []
    total_rows = 5
    try:
        await _wait_slot_active(postgres_dsn, slot_name)

        # Insert one row at a time, waiting for it to be delivered before the
        # next commit -- this keeps the queue empty at every point we might
        # call close(), so the assertion below isn't racing the
        # "queued-but-unstarted work is dropped on immediate shutdown"
        # behavior (that race is what test_sigterm_under_backpressure below
        # deliberately exercises instead).
        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            for i in range(total_rows):
                entity_id = f"tx-{i}"
                await conn.execute(_insert_row(entity_id))
                committed_ids.append(entity_id)
                await _wait_for_count(delivered, len(committed_ids))

        client.close()

        await asyncio.wait_for(run_task, timeout=10.0)

        assert [_entity_id(tx) for tx in delivered] == committed_ids
    finally:
        await _cleanup(client, run_task)


@pytest.mark.timeout(30)
async def test_sigterm_while_processing_a_transaction(
    postgres_dsn, outbox_table, tmp_path
):
    slot_name = _unique_slot_name()
    checkpoint_path = tmp_path / "checkpoint"
    client = ReplicationClient(_options(postgres_dsn, slot_name, checkpoint_path))

    started = asyncio.Event()
    release = asyncio.Event()
    side_effects: list[str] = []
    processed_lsn: list[int] = []

    async def handler(transaction: Transaction, checkpoint: CheckpointHandle) -> None:
        started.set()
        await release.wait()
        side_effects.append(_entity_id(transaction))
        processed_lsn.append(transaction.commit_lsn)
        await checkpoint.save(transaction.commit_lsn)

    run_task = asyncio.ensure_future(client.run(handler))
    try:
        await _wait_slot_active(postgres_dsn, slot_name)

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(_insert_row("processing-during-shutdown"))

        await asyncio.wait_for(started.wait(), timeout=5.0)
        client.close()

        # close() must not interrupt the handler already in flight.
        await asyncio.sleep(0.2)
        assert not run_task.done()
        assert side_effects == []

        release.set()
        await asyncio.wait_for(run_task, timeout=10.0)

        assert side_effects == ["processing-during-shutdown"]
        assert await FileCheckpointStore(checkpoint_path).load() == processed_lsn[0]
    finally:
        await _cleanup(client, run_task)


@pytest.mark.timeout(30)
async def test_sigterm_under_backpressure(postgres_dsn, outbox_table, tmp_path):
    slot_name = _unique_slot_name()
    checkpoint_path = tmp_path / "checkpoint"
    client = ReplicationClient(
        _options(
            postgres_dsn,
            slot_name,
            checkpoint_path,
            max_pending_transactions=1,
        )
    )

    started = asyncio.Event()
    release = asyncio.Event()
    processed: list[str] = []
    processed_lsn: list[int] = []

    async def handler(transaction: Transaction, checkpoint: CheckpointHandle) -> None:
        started.set()
        await release.wait()
        processed.append(_entity_id(transaction))
        processed_lsn.append(transaction.commit_lsn)
        await checkpoint.save(transaction.commit_lsn)

    run_task = asyncio.ensure_future(client.run(handler))
    total_rows = (
        4  # one in flight + one queued + one blocking the receiver + one unread
    )
    try:
        await _wait_slot_active(postgres_dsn, slot_name)

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            for i in range(total_rows):
                await conn.execute(_insert_row(f"bp-{i}"))

        await asyncio.wait_for(started.wait(), timeout=5.0)
        await asyncio.sleep(0.3)  # let the receiver race ahead and fill the queue

        client.close()
        release.set()  # let the one in-flight handler finish

        await asyncio.wait_for(run_task, timeout=10.0)

        assert processed == ["bp-0"]
        assert await FileCheckpointStore(checkpoint_path).load() == processed_lsn[0]
    finally:
        await _cleanup(client, run_task)

    # Whatever was still queued-but-never-started (or never even read off the
    # wire) was safely dropped, not lost -- it was never checkpointed, so a
    # fresh client against the same slot and checkpoint store redelivers it.
    redelivered: list[str] = []

    async def redeliver_handler(
        transaction: Transaction, checkpoint: CheckpointHandle
    ) -> None:
        redelivered.append(_entity_id(transaction))

    fresh_client = ReplicationClient(_options(postgres_dsn, slot_name, checkpoint_path))
    fresh_task = asyncio.ensure_future(fresh_client.run(redeliver_handler))
    try:
        await _wait_for_count(redelivered, total_rows - 1, timeout=10.0)
        assert redelivered == [f"bp-{i}" for i in range(1, total_rows)]
    finally:
        await _cleanup(fresh_client, fresh_task)
