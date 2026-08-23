"""Runnable example of the same-transaction checkpoint pattern against Postgres.

See README.md's "Exactly-once effects" section for the pattern this
demonstrates. Run these once, manually, against your database before running
this script -- walbox creates its replication slot idempotently, but never
creates or alters the publication itself (see README.md's PostgreSQL
configuration section):

CREATE TABLE outbox (
    id          BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE PUBLICATION walbox_pub FOR TABLE outbox;

-- The downstream Postgres sink this example writes to -- a projection built
-- from outbox inserts, entirely separate from the outbox table itself.
CREATE TABLE outbox_projection (
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

import asyncio
import functools
import logging
import os
import signal

import psycopg

from walbox import ChangeKind
from walbox import CheckpointHandle
from walbox import PostgresCheckpointStore
from walbox import ReplicationClient
from walbox import ReplicationOptions
from walbox import Transaction

logger = logging.getLogger("walbox.examples.outbox_postgres")


async def handle(
    tx: Transaction,
    checkpoint: CheckpointHandle,
    *,
    dsn: str,
) -> None:
    """Write to `outbox_projection` and checkpoint in one Postgres commit.

    `PostgresCheckpointStore.save` only skips committing when it's given a
    `connection=` -- passed here, so the upsert runs uncommitted on `conn`
    instead of on a throwaway connection of its own. That's the *only* way to
    get a downstream Postgres write and the checkpoint into the same commit:
    a crash between them is impossible, either both happened or neither did.

    Reach for this shape instead of `outbox.handle`'s whenever "publish"
    means writing to another table in this same database (e.g. a
    projection), rather than an external broker -- only a Postgres write can
    share a transaction with the checkpoint this way.
    """
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        for change in tx.changes:
            if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
                continue

            new = change.new
            await conn.execute(
                "INSERT INTO outbox_projection "
                "(entity_type, entity_id, event_type, payload) "
                "VALUES (%s, %s, %s, %s)",
                (
                    new["entity_type"],
                    new["entity_id"],
                    new["event_type"],
                    psycopg.types.json.Jsonb(new["payload"]),
                ),
            )

        await checkpoint.save(tx.commit_lsn, connection=conn)
        await conn.commit()


async def main() -> None:
    """Connect, subscribe, and dispatch outbox transactions until stopped."""
    dsn = os.environ["WALBOX_DSN"]
    consumer_name = "outbox-postgres-consumer"
    checkpoint_store = PostgresCheckpointStore(dsn, consumer_name=consumer_name)
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

    await client.run(functools.partial(handle, dsn=dsn))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
