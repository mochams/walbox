"""Sharded, order-preserving concurrency on top of walbox's single consumer.

walbox hands transactions to one handler, one at a time, in commit order
(Backpressure, RFC 06) -- that's what keeps checkpointing simple and correct,
but it also means a slow handler body processes everything it's given
strictly sequentially unless the handler itself introduces concurrency.

This example fans the *changes inside one transaction* out across a fixed
number of shard queues, each drained by its own worker task. Two events for
different entities land on different shards and process concurrently; two
events for the *same* entity always hash to the same shard and are drained
strictly in order by that shard's single worker, so per-entity ordering
survives no matter how many shards there are. `checkpoint.save()` only runs
once every shard has drained everything this transaction just queued --
walbox never has more than one `handle()` call in flight at a time, so a
transaction that fans out across many shards makes that one call (and, if
`close()` fires mid-flight, the graceful shutdown waiting on it) take
proportionally longer. That's inherent to only checkpointing once the
fan-out is confirmed done, not a bug: checkpointing any earlier would risk
claiming durable progress for work that hasn't actually finished yet.

Requires `psycopg-pool` (`pip install psycopg-pool`) -- a separate package
from `psycopg` itself. walbox's own dependency footprint stays at just
`psycopg`; `from_pool` and this handler only need *something* shaped like a
pool, so bringing one is the application's choice, not walbox's.

Run these once, manually, against your database before running this script --
walbox creates its replication slot idempotently, but never creates or alters
the publication itself (see README.md's PostgreSQL configuration section):

CREATE TABLE outbox (
    id          BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE PUBLICATION walbox_pub FOR TABLE outbox;
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
from walbox import PostgresCheckpointStore
from walbox import ReplicationClient
from walbox import ReplicationOptions
from walbox import Transaction

logger = logging.getLogger("walbox.examples.outbox_concurrency")


async def publish_to_broker(payload: dict[str, Any]) -> None:
    """Stand-in for a real message-broker publish call."""
    await asyncio.sleep(0.1)  # pretend it takes a moment to publish
    logger.info("publishing: %s", payload)


class ShardedHandler:
    """Routes each outbox insert to one of `shard_count` FIFO shard queues.

    Routing key is a stable hash of `entity_id`, so the same entity always
    lands on the same shard (order preserved) while different entities can
    spread across shards (processed concurrently).
    """

    def __init__(self, shard_count: int = 4, queue_maxsize: int = 50) -> None:
        """Initialize with the shard count and each shard's queue bound.

        `queue_maxsize` bounds how many not-yet-processed events one shard
        can hold before `handle()` blocks on `put()` -- a low value keeps
        memory bounded under a slow `publish_to_broker`, same reasoning as
        walbox's own `max_pending_transactions`.
        """
        self._shard_count = shard_count
        self._shards: list[asyncio.Queue[dict[str, Any]]] = [
            asyncio.Queue(maxsize=queue_maxsize) for _ in range(shard_count)
        ]
        self._workers: list[asyncio.Task[None]] = []

    def start(self) -> None:
        """Start one worker task per shard. Call once, before `handle` runs."""
        self._workers = [
            asyncio.create_task(self._shard_worker(i), name=f"shard-worker-{i}")
            for i in range(self._shard_count)
        ]

    async def stop(self, *, drain_timeout: float = 5.0) -> None:
        """Let in-flight shard work finish, then cancel and await every worker.

        Call once, after `client.run` returns -- nothing can put new work onto
        a shard queue by then, since only `handle` does that and it can't
        still be running once `client.run` has returned or raised. On a
        clean shutdown `handle`'s own `join()` calls already drained every
        shard, so this resolves immediately; it earns its keep on the
        *unclean* path (`client.run` raising mid-transaction), where a
        worker could otherwise be cancelled mid-`_process_event` instead of
        being allowed to finish what it already started. `drain_timeout`
        bounds how long a genuinely stuck worker (e.g. a hung
        `publish_to_broker`) can delay exit -- it's cancelled anyway once
        that elapses.
        """
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

        `hashlib.md5`, not the builtin `hash()`: Python randomizes `str`
        hashing per process by default (`PYTHONHASHSEED`), so `hash()` would
        route the same entity to a different shard on every restart,
        breaking the ordering guarantee this class exists to provide.
        `usedforsecurity=False` because this is routing, not cryptography.

        Returns:
            The shard index `entity_id` is assigned to.
        """
        digest = hashlib.md5(entity_id.encode(), usedforsecurity=False).hexdigest()
        return int(digest, 16) % self._shard_count

    async def _shard_worker(self, shard_id: int) -> None:
        """Drain one shard's queue forever, one event at a time, in order.

        A failure in `_process_event` is logged and the queue moves on to
        the next item -- one bad event degrades that shard, it never kills
        the worker task. A worker task that dies here would leave `join()`
        blocked forever on every future transaction, since nothing would be
        left to call `task_done()` for whatever it was holding.
        """
        logger.info("shard worker %d started", shard_id)
        queue = self._shards[shard_id]
        while True:
            payload = await queue.get()
            try:
                await self._process_event(shard_id, payload)
            except Exception:
                logger.exception("shard %d failed to process an event", shard_id)
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
        """Fan this transaction's outbox inserts across shards, then checkpoint."""
        touched_shards: set[int] = set()
        for change in tx.changes:
            if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
                continue

            payload = change.new

            shard_id = self._shard_for(payload["entity_id"])
            touched_shards.add(shard_id)
            await self._shards[shard_id].put(payload)

        for shard_id in touched_shards:
            await self._shards[shard_id].join()

        await checkpoint.save(tx.commit_lsn)


async def main() -> None:
    """Connect, subscribe, and dispatch outbox transactions until stopped."""
    dsn = os.environ["WALBOX_DSN"]
    consumer_name = "outbox-concurrency-consumer"

    # min_size/max_size are usage-dependent; these are just reasonable
    # example defaults, not a recommendation for any particular workload.
    async with AsyncConnectionPool(dsn, min_size=1, max_size=5, open=False) as pool:
        checkpoint_store = PostgresCheckpointStore.from_pool(
            pool,
            consumer_name=consumer_name,
        )
        options = ReplicationOptions(
            consumer_name=consumer_name,
            dsn=dsn,
            slot_name="outbox_slot",
            publication_name="walbox_pub",
            checkpoint_store=checkpoint_store,
        )
        client = ReplicationClient(options)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, client.close)

        sharded = ShardedHandler(shard_count=4)
        sharded.start()

        try:
            await client.run(sharded.handle)
        finally:
            await sharded.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
