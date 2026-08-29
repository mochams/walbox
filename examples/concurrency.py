"""Sharded, order-preserving concurrency on top of walbox's single consumer.

walbox hands transactions to one handler at a time, in commit order. This
fans one transaction's changes out across shard queues keyed by a stable
hash of `entity_id`: different entities process concurrently, while events
for the same entity stay strictly in order. `checkpoint.save()` only runs
once every touched shard has drained, so checkpointing never claims progress
on work that hasn't finished.

Only pays off when one transaction touches many entities at once (a bulk
insert or backfill). For one row per commit, sharding is pure overhead.

Requires `pip install psycopg-pool`.

Run once before starting (walbox creates the replication slot, not the
publication):

CREATE TABLE published_table (
    id          BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE PUBLICATION walbox_pub FOR TABLE published_table;
"""

import asyncio
import contextlib
import hashlib
import logging
import os
import signal
from typing import Any

from psycopg_pool import AsyncConnectionPool

from walbox import ChangeKind
from walbox import CheckpointHandle
from walbox import Transaction
from walbox import Walbox
from walbox import WalboxOptions

logger = logging.getLogger("walbox.examples.concurrency")


async def publish_to_broker(payload: dict[str, Any]) -> None:
    """Stand-in for a real message-broker publish call."""
    await asyncio.sleep(0.1)  # pretend it takes a moment to publish
    logger.info("publishing: %s", payload)


class ConcurrentHandler:
    """Routes each insert to one of `shard_count` FIFO shard queues.

    A stable hash of `entity_id` picks the shard, so the same entity always
    lands on the same shard (order preserved) while different entities
    spread across shards (processed concurrently).
    """

    def __init__(self, shard_count: int = 4, queue_maxsize: int = 50) -> None:
        """Initialize with the shard count and each shard's queue bound."""
        self._shard_count = shard_count
        self._shards: list[asyncio.Queue[dict[str, Any]]] = [
            asyncio.Queue(maxsize=queue_maxsize) for _ in range(shard_count)
        ]
        self._workers: list[asyncio.Task[None]] = []
        # queue.join() has no concept of success/failure; failures land here
        # so `handle` can raise instead of silently checkpointing past them.
        self._failures: list[BaseException] = []

    def start(self) -> None:
        """Start one worker task per shard. Call once, before `handle` runs."""
        self._workers = [
            asyncio.create_task(self._shard_worker(i), name=f"shard-worker-{i}")
            for i in range(self._shard_count)
        ]

    async def stop(self, *, drain_timeout: float = 5.0) -> None:
        """Drain each shard, then cancel and await every worker."""
        for shard in self._shards:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(shard.join(), timeout=drain_timeout)
        for worker in self._workers:
            worker.cancel()
        for worker in self._workers:
            with contextlib.suppress(asyncio.CancelledError):
                await worker

    def _shard_for(self, entity_id: str) -> int:
        """Hash `entity_id` to a stable shard index.

        `hashlib.md5`, not the builtin `hash()`: `hash()` is randomized per
        process (`PYTHONHASHSEED`), which would break the ordering guarantee
        across restarts. `usedforsecurity=False` since this is routing, not
        cryptography.

        Returns:
            The shard index `entity_id` is assigned to.
        """
        digest = hashlib.md5(entity_id.encode(), usedforsecurity=False).hexdigest()
        return int(digest, 16) % self._shard_count

    async def _shard_worker(self, shard_id: int) -> None:
        """Drain one shard's queue forever, recording failures without dying."""
        logger.info("shard worker %d started", shard_id)
        queue = self._shards[shard_id]
        while True:
            payload = await queue.get()
            try:
                await self._process_event(shard_id, payload)
            except Exception as exc:
                logger.exception(
                    "shard %d failed to process %s for entity %s",
                    shard_id,
                    payload.get("event_type", "unknown"),
                    payload.get("entity_id", "unknown"),
                )
                self._failures.append(exc)
            finally:
                queue.task_done()

    async def _process_event(self, shard_id: int, payload: dict[str, Any]) -> None:
        entity_id = payload.get("entity_id", "unknown")
        event_type = payload.get("event_type", "unknown")
        logger.info(
            "shard %d processing %s for entity %s",
            shard_id,
            event_type,
            entity_id,
        )
        await publish_to_broker(payload)

    async def handle(self, tx: Transaction, checkpoint: CheckpointHandle) -> None:
        """Fan this transaction's inserts across shards, then checkpoint.

        Raises if any shard worker failed on an event from this transaction,
        so the whole transaction is redelivered instead of silently
        checkpointed past a failure.

        Raises:
            ExceptionGroup: If one or more shard workers failed to process
                an event from this transaction.
        """
        touched_shards: set[int] = set()
        for change in tx.changes:
            if (
                change.table != "public.published_table"
                or change.kind != ChangeKind.INSERT
            ):
                continue

            payload = change.new

            shard_id = self._shard_for(payload["entity_id"])
            touched_shards.add(shard_id)
            await self._shards[shard_id].put(payload)

        for shard_id in touched_shards:
            await self._shards[shard_id].join()

        if self._failures:
            failures, self._failures = self._failures, []
            message = f"{len(failures)} event(s) failed while processing xid {tx.xid}"
            raise ExceptionGroup(message, failures)

        await checkpoint.save(tx.commit_lsn)


async def main() -> None:
    """Build the client and run it until stopped."""
    dsn = os.environ["WALBOX_DSN"]
    options = WalboxOptions(
        consumer_name="concurrency-consumer",
        dsn=dsn,
        slot_name="published_table_slot",
        publication_name="walbox_pub",
    )

    # min_size/max_size are usage-dependent; these are just reasonable
    # example defaults, not a recommendation for any particular workload.
    async with AsyncConnectionPool(dsn, min_size=1, max_size=5) as pool:
        client = Walbox.build_with_pool(options, pool)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, client.close)

        handler = ConcurrentHandler(shard_count=4)
        handler.start()

        try:
            await client.run(handler.handle)
        finally:
            await handler.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
