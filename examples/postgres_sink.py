"""Runnable example of the same-transaction checkpoint pattern against Postgres.

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

-- The downstream Postgres sink this example writes to: a projection built
-- from published_table inserts, entirely separate from the table itself.
CREATE TABLE published_table_projection (
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
from psycopg_pool import AsyncConnectionPool

from walbox import ChangeKind
from walbox import CheckpointHandle
from walbox import Transaction
from walbox import WalboxBuilder
from walbox import WalboxOptions

logger = logging.getLogger("walbox.examples.postgres_sink")


async def handle(
    tx: Transaction,
    checkpoint: CheckpointHandle,
    *,
    pool: AsyncConnectionPool,
) -> None:
    """Write to `published_table_projection` and checkpoint in one Postgres commit.

    `connection=conn` makes the write and the checkpoint atomic; `conn` comes
    from the pool instead of a fresh connection per transaction.
    """
    async with pool.connection() as conn:
        for change in tx.changes:
            if (
                change.table != "public.published_table"
                or change.kind != ChangeKind.INSERT
            ):
                continue

            new = change.new
            await conn.execute(
                "INSERT INTO published_table_projection "
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
    """Build the client and run it until stopped."""
    dsn = os.environ["WALBOX_DSN"]
    options = WalboxOptions(
        consumer_name="postgres-sink-consumer",
        dsn=dsn,
        slot_name="published_table_slot",
        publication_name="walbox_pub",
    )

    # min_size/max_size are usage-dependent; these are just reasonable
    # example defaults, not a recommendation for any particular workload.
    async with AsyncConnectionPool(dsn, min_size=1, max_size=5) as pool:
        client = WalboxBuilder.build_with_pool(options, pool)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, client.close)

        handle_with_pool = functools.partial(handle, pool=pool)
        await client.run(handle_with_pool)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
