"""Integration tests for bounded backpressure against a real Postgres.

A tiny `max_pending_transactions` and a handler paused on a test-controlled
`asyncio.Event` reproduce the guide's "slow handler" scenario: the client's
own queue must stay bounded, feedback must not advance past what is
actually checkpointed, and PostgreSQL's own view of replication lag must
grow instead -- proving backpressure is real, not just locally invisible.
"""

import asyncio
import contextlib
import uuid
from unittest.mock import AsyncMock

import pytest
from psycopg import AsyncConnection

from walbox.abc import CheckpointHandle
from walbox.abc import Transaction
from walbox.abc import WalboxOptions
from walbox.checkpoint import PostgresCheckpointStore
from walbox.client import WalboxClient

pytestmark = pytest.mark.postgres

_STATUS_INTERVAL = 1
_MAX_PENDING = 3


def _unique_slot_name() -> str:
    return f"slot_{uuid.uuid4().hex}"


def _unique_consumer_name() -> str:
    return f"consumer_{uuid.uuid4().hex}"


async def _insert_rows(dsn: str, count: int) -> None:
    async with await AsyncConnection.connect(dsn, autocommit=True) as conn:
        for i in range(count):
            await conn.execute(
                "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
                f"VALUES ('user', 'tx-{i}', 'user_created', '{{}}'::jsonb)",
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


async def _lag_bytes(dsn: str, slot_name: str) -> int:
    async with await AsyncConnection.connect(dsn, autocommit=True) as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT pg_current_wal_lsn() - confirmed_flush_lsn "
                "FROM pg_replication_slots WHERE slot_name = %s",
                (slot_name,),
            )
            row = await cur.fetchone()
    assert row is not None
    return int(row[0])


def _decode_flushed_lsn(payload: bytes) -> int:
    return int.from_bytes(payload[9:17], "big") - 1


def _options(
    postgres_dsn: str,
    slot_name: str,
    consumer_name: str,
) -> WalboxOptions:
    return WalboxOptions(
        consumer_name=consumer_name,
        dsn=postgres_dsn,
        slot_name=slot_name,
        publication_name="walbox_pub",
        max_pending_transactions=_MAX_PENDING,
        status_interval=_STATUS_INTERVAL,
    )


class _RunningClient:
    """Runs a `WalboxClient` as a background task for one test's lifetime."""

    def __init__(self, client: WalboxClient, handler) -> None:
        self._client = client
        self._task = asyncio.ensure_future(client.run(handler))

    def raise_if_failed(self) -> None:
        if self._task.done() and not self._task.cancelled():
            exc = self._task.exception()
            if exc is not None:
                raise exc

    async def stop(self) -> None:
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task
        if self._client._transport is not None:
            self._client._transport.close()


class _QueueSizeSampler:
    """Polls a client's queue depth in the background until stopped."""

    def __init__(self, client: WalboxClient, interval: float = 0.02) -> None:
        self.samples: list[int] = []
        self._task = asyncio.ensure_future(self._run(client, interval))

    async def _run(self, client: WalboxClient, interval: float) -> None:
        while True:
            self.samples.append(client._queue.qsize())
            await asyncio.sleep(interval)

    async def stop(self) -> None:
        self._task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._task


@pytest.mark.timeout(30)
async def test_bounded_queue_keeps_memory_bounded_under_a_slow_handler(
    postgres_dsn,
    outbox_table,
):
    slot_name = _unique_slot_name()
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(transaction: Transaction, checkpoint: CheckpointHandle) -> None:
        started.set()
        await release.wait()

    consumer_name = _unique_consumer_name()
    client = WalboxClient(
        _options(postgres_dsn, slot_name, consumer_name),
        checkpoint_store=PostgresCheckpointStore(
            postgres_dsn,
            consumer_name=consumer_name,
        ),
    )
    running = _RunningClient(client, handler)
    sampler = _QueueSizeSampler(client)
    try:
        await _wait_slot_active(postgres_dsn, slot_name)

        await _insert_rows(postgres_dsn, _MAX_PENDING * 3)

        await asyncio.wait_for(started.wait(), timeout=5.0)
        await asyncio.sleep(0.5)  # let the receiver race ahead and fill the queue
        running.raise_if_failed()

        assert sampler.samples
        assert max(sampler.samples) <= _MAX_PENDING

        release.set()
    finally:
        await sampler.stop()
        await running.stop()


@pytest.mark.timeout(30)
async def test_feedback_does_not_advance_while_backpressured(
    postgres_dsn,
    outbox_table,
):
    slot_name = _unique_slot_name()
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(transaction: Transaction, checkpoint: CheckpointHandle) -> None:
        started.set()
        await release.wait()

    consumer_name = _unique_consumer_name()
    client = WalboxClient(
        _options(postgres_dsn, slot_name, consumer_name),
        checkpoint_store=PostgresCheckpointStore(
            postgres_dsn,
            consumer_name=consumer_name,
        ),
    )

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

        await _insert_rows(postgres_dsn, _MAX_PENDING * 3)

        await asyncio.wait_for(started.wait(), timeout=5.0)
        write_calls.clear()
        await asyncio.sleep((_STATUS_INTERVAL * 2) + 1)
        running.raise_if_failed()

        assert write_calls
        assert all(_decode_flushed_lsn(payload) == 0 for payload in write_calls)

        release.set()
    finally:
        await running.stop()


@pytest.mark.timeout(30)
async def test_replication_lag_grows_instead_of_the_process_buffering_unboundedly(
    postgres_dsn,
    outbox_table,
):
    slot_name = _unique_slot_name()
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(transaction: Transaction, checkpoint: CheckpointHandle) -> None:
        started.set()
        await release.wait()

    consumer_name = _unique_consumer_name()
    client = WalboxClient(
        _options(postgres_dsn, slot_name, consumer_name),
        checkpoint_store=PostgresCheckpointStore(
            postgres_dsn,
            consumer_name=consumer_name,
        ),
    )
    running = _RunningClient(client, handler)
    sampler = _QueueSizeSampler(client)
    try:
        await _wait_slot_active(postgres_dsn, slot_name)
        initial_lag = await _lag_bytes(postgres_dsn, slot_name)

        await _insert_rows(postgres_dsn, _MAX_PENDING * 3)

        await asyncio.wait_for(started.wait(), timeout=5.0)
        await asyncio.sleep((_STATUS_INTERVAL * 3) + 1)
        running.raise_if_failed()

        assert sampler.samples
        assert max(sampler.samples) <= _MAX_PENDING

        grown_lag = await _lag_bytes(postgres_dsn, slot_name)
        assert grown_lag > initial_lag

        release.set()
    finally:
        await sampler.stop()
        await running.stop()
