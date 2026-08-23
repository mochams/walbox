"""Runnable transactional-outbox example using a pooled checkpoint store.

Identical to outbox.py's broker pattern, except `PostgresCheckpointStore` is
built via `from_pool` instead of a bare DSN: `checkpoint.save(...)` (called
once per transaction, with no `connection=`) reuses a connection from an
application-managed pool instead of opening a fresh one every time. See
[RFC 01](../docs/rfc-01-checkpoint-store.md) for why `from_pool` exists.

Requires `psycopg-pool` (`pip install psycopg-pool`) -- a separate package
from `psycopg` itself. walbox's own dependency footprint stays at just
`psycopg`; `from_pool` only needs *something* shaped like a pool, so
bringing one is the application's choice, not walbox's.

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

logger = logging.getLogger("walbox.examples.outbox_pool")


async def publish_to_broker(payload: dict[str, Any]) -> None:
    """Stand-in for a real message-broker publish call."""
    await asyncio.sleep(0.1)  # pretend it takes a moment to publish
    logger.info("publishing: %s", payload)


async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    """Publish each outbox insert, then durably checkpoint the transaction.

    Word-for-word the same as outbox.py's `handle` -- the pool lives inside
    `checkpoint`'s underlying `PostgresCheckpointStore`, so this handler
    doesn't need to know or care that one is in play. Swapping a plain
    `PostgresCheckpointStore(dsn, ...)` for `from_pool(pool, ...)` in
    `main()` below is the entire change.
    """
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue

        await publish_to_broker(change.new)

    await checkpoint.save(tx.commit_lsn)


async def main() -> None:
    """Connect, subscribe, and dispatch outbox transactions until stopped."""
    dsn = os.environ["WALBOX_DSN"]
    consumer_name = "outbox-pool-consumer"

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

        await client.run(handle)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
