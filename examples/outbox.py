"""Runnable transactional-outbox example built on walbox.

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

import psycopg

from walbox import ChangeKind
from walbox import PostgresCheckpointStore
from walbox import ReplicationClient
from walbox import ReplicationOptions
from walbox import Transaction

logger = logging.getLogger("walbox.examples.outbox")


async def publish_to_broker(payload: dict[str, Any]) -> None:
    """Stand-in for a real message-broker publish call."""
    await asyncio.sleep(0.1)  # pretend it takes a moment to publish
    logger.info("publishing: %s", payload)


async def handle(tx: Transaction) -> None:
    """Publish each outbox insert, then durably checkpoint the transaction.

    `publish_to_broker` may be retried on redelivery after a crash (see
    README.md's failure-semantics table), so a real broker publish should
    dedupe on `payload["id"]`, the outbox row's natural event ID.

    `tx.checkpoint.save(tx.commit_lsn)` below is called with no `connection=`,
    so it always opens its own connection and commits immediately (see
    `PostgresCheckpointStore.save`) -- it can never be atomic with anything
    else this handler does. That's fine here because the broker is an
    external system that could never share a Postgres transaction anyway, so
    redelivery-with-dedupe is the only available correctness strategy. When
    the downstream write is itself Postgres, use
    `handle_with_atomic_checkpoint` below instead, which gets real atomicity
    by passing its own connection into `checkpoint.save`.
    """
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue

        await publish_to_broker(change.new)

    await tx.checkpoint.save(tx.commit_lsn)


async def handle_with_atomic_checkpoint(tx: Transaction, dsn: str) -> None:
    """Commit a Postgres write and the checkpoint update atomically.

    `PostgresCheckpointStore.save` only skips committing when it's given a
    `connection=` -- pass one, and the upsert runs uncommitted on that
    connection instead of on a throwaway one of its own. That's the *only*
    way to get a downstream Postgres write and the checkpoint into the same
    commit; `handle`'s plain `checkpoint.save(tx.commit_lsn)` call always
    commits on its own separate connection and can't be made atomic with
    anything else no matter what the handler does first.

    This demonstrates the same-transaction pattern (README.md's
    exactly-once-effects section): a handler's own downstream Postgres
    write(s) and `checkpoint.save` share one connection and one commit, so a
    crash between them is impossible -- either both happened, or neither
    did. Reach for this shape instead of `handle`'s whenever "publish" means
    writing to another table in this same database (e.g. a projection),
    rather than an external broker -- only a Postgres write can share a
    transaction with the checkpoint this way.

    This example has no downstream write of its own -- add yours where
    noted below, iterating `tx.changes` the same way `handle` does above,
    e.g.:

        await conn.execute("INSERT INTO projection (...) VALUES (...)", (...))

    It must run on `conn`, uncommitted, so it becomes durable together with
    the checkpoint below -- never on a separate connection or transaction.

    Not wired into `main()` below. `client.run()` always calls its handler
    with a single `Transaction` argument, so using this one instead of
    `handle` means binding `dsn` first, e.g.:

        await client.run(functools.partial(handle_with_atomic_checkpoint, dsn=dsn))
    """
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        # Your own downstream write(s) go here.
        await tx.checkpoint.save(tx.commit_lsn, connection=conn)
        await conn.commit()


async def main() -> None:
    """Connect, subscribe, and dispatch outbox transactions until stopped."""
    dsn = os.environ["WALBOX_DSN"]
    consumer_name = "outbox-consumer"
    checkpoint_store = PostgresCheckpointStore(dsn, consumer_name=consumer_name)
    options = ReplicationOptions(
        consumer_name=consumer_name,
        dsn=dsn,
        slot_name="outbox_slot",
        publication_name="walbox_pub",
        checkpoint_store=checkpoint_store,
        manage_checkpoint=False,  # handle() checkpoints explicitly, after publishing.
    )
    client = ReplicationClient(options)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, client.close)

    # Swap for handle_with_atomic_checkpoint (see its docstring) if your
    # downstream write is itself Postgres and you want it committed
    # atomically with the checkpoint.
    await client.run(handle)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
