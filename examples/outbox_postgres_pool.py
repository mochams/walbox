"""Runnable example of the same-transaction checkpoint pattern, pooled.

Identical to outbox_postgres.py's atomic same-transaction pattern, except the
handler checks out its connection from an application-managed
`psycopg_pool.AsyncConnectionPool` instead of opening a fresh connection per
transaction, and `PostgresCheckpointStore` is built via `from_pool` against
that same pool -- so the only connection this script opens outside the pool
is the replication connection itself. See
[RFC 01](../docs/rfc-01-checkpoint-store.md) for why `from_pool` exists.

The handler needs the pool at call time, but `ReplicationClient.run()` always
calls its handler with a single `(Transaction, CheckpointHandle)` pair --
`functools.partial` is how `pool` gets bound ahead of time, the same way
outbox_postgres.py binds `dsn`.

Worth being precise about: since this handler always passes `connection=conn`
to `checkpoint.save`, `PostgresCheckpointStore`'s own pooled `_acquire` is
never exercised by `save()` here -- only by `ReplicationClient.run()`'s one
`load()` call at startup. The per-transaction connection reuse visible below
comes from the handler checking out `pool.connection()` directly, which
doesn't depend on `from_pool` at all. `from_pool` earns its keep on *every*
transaction in `outbox_pool.py` instead, whose handler calls
`checkpoint.save(tx.commit_lsn)` with no `connection=`.

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
from psycopg_pool import AsyncConnectionPool

from walbox import ChangeKind
from walbox import CheckpointHandle
from walbox import PostgresCheckpointStore
from walbox import ReplicationClient
from walbox import ReplicationOptions
from walbox import Transaction

logger = logging.getLogger("walbox.examples.outbox_postgres_pool")


async def handle(
    tx: Transaction,
    checkpoint: CheckpointHandle,
    *,
    pool: AsyncConnectionPool,
) -> None:
    """Write to `outbox_projection` and checkpoint in one Postgres commit.

    Same pattern as outbox_postgres.py's handler -- `checkpoint.save`'s
    `connection=` is what makes the downstream write and the checkpoint
    atomic, regardless of where `conn` came from. The only difference here
    is `conn` comes from `pool.connection()` (checked out for this
    transaction, returned to the pool on block exit) instead of a fresh
    `psycopg.AsyncConnection.connect(dsn)` call every time.
    """
    async with pool.connection() as conn:
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
    consumer_name = "outbox-postgres-pool-consumer"

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

        await client.run(
            functools.partial(handle, pool=pool),
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
