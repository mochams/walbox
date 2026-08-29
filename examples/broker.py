"""Runnable example publishing changes from a published table to an external broker.

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
import logging
import os
import signal
from typing import Any

from psycopg_pool import AsyncConnectionPool

from walbox import ChangeKind
from walbox import CheckpointHandle
from walbox import Transaction
from walbox import WalboxBuilder
from walbox import WalboxOptions

logger = logging.getLogger("walbox.examples.broker")


async def publish_to_broker(payload: dict[str, Any]) -> None:
    """Stand-in for a real message-broker publish call."""
    await asyncio.sleep(0.1)  # pretend it takes a moment to publish
    logger.info("publishing: %s", payload)


async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    """Publish each insert, then checkpoint the transaction.

    A crash before the checkpoint redelivers the transaction, so a real
    broker publish should dedupe on `payload["id"]`.
    """
    for change in tx.changes:
        if change.table != "public.published_table" or change.kind != ChangeKind.INSERT:
            continue

        await publish_to_broker(change.new)

    await checkpoint.save(tx.commit_lsn)


async def main() -> None:
    """Build the client and run it until stopped."""
    dsn = os.environ["WALBOX_DSN"]
    options = WalboxOptions(
        consumer_name="broker-consumer",
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

        await client.run(handle)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
