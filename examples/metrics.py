"""Runnable example contrasting a blocking and a non-blocking `on_metrics` callback.

`on_metrics` is called synchronously, on the same event loop as everything
else walbox does. A callback that blocks (a synchronous HTTP call, a
`time.sleep`, disk I/O) stalls replication reads and handler dispatch for
as long as it runs, not just the metrics export. This example runs the
non-blocking version; the blocking one is included only to show what not
to do.

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
import logging
import os
import signal
import time

from psycopg_pool import AsyncConnectionPool

from walbox import ChangeKind
from walbox import CheckpointHandle
from walbox import Metrics
from walbox import Transaction
from walbox import Walbox
from walbox import WalboxOptions

logger = logging.getLogger("walbox.examples.metrics")


def on_metrics_bad(metrics: Metrics) -> None:
    """BAD: blocks the event loop. Don't do this.

    `on_metrics` runs synchronously, inline with everything else on the
    loop. A blocking call here, real HTTP client, `time.sleep`, disk
    write, stalls replication reads and handler dispatch until it
    returns, even though it has nothing to do with either.
    """
    time.sleep(0.2)  # stand-in for a blocking HTTP POST to a metrics backend
    logger.info("sent metrics the slow way: queue_depth=%d", metrics.queue_depth)


async def export_metrics(queue: asyncio.Queue[Metrics]) -> None:
    """Background task that actually sends metrics, off the hot path."""
    while True:
        metrics = await queue.get()
        try:
            # Real code would call Prometheus, StatsD, CloudWatch, etc. here.
            logger.info(
                "exported metrics: queue_depth=%d lag_bytes=%d",
                metrics.queue_depth,
                metrics.replication_lag_bytes,
            )
        except Exception:
            logger.exception("failed to export metrics")


def make_on_metrics_good(queue: asyncio.Queue[Metrics]):
    """GOOD: hand the snapshot off to a queue and return immediately.

    Returns:
        A callback that enqueues each `Metrics` snapshot without blocking.
    """

    def on_metrics_good(metrics: Metrics) -> None:
        # Drop this snapshot rather than block if the queue is full; the
        # next one follows soon.
        with contextlib.suppress(asyncio.QueueFull):
            queue.put_nowait(metrics)

    return on_metrics_good


async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    """Log inserts, then checkpoint. Metrics flow through `on_metrics`, not here."""
    for change in tx.changes:
        if change.table != "public.published_table" or change.kind != ChangeKind.INSERT:
            continue
        logger.info("processed: %s", change.new)

    await checkpoint.save(tx.commit_lsn)


async def main() -> None:
    """Build the client with the non-blocking metrics callback and run it."""
    dsn = os.environ["WALBOX_DSN"]
    metrics_queue: asyncio.Queue[Metrics] = asyncio.Queue(maxsize=100)

    options = WalboxOptions(
        consumer_name="metrics-consumer",
        dsn=dsn,
        slot_name="published_table_slot",
        publication_name="walbox_pub",
        on_metrics=make_on_metrics_good(metrics_queue),
    )

    exporter = asyncio.create_task(export_metrics(metrics_queue))

    async with AsyncConnectionPool(dsn, min_size=1, max_size=5) as pool:
        client = Walbox.build_with_pool(options, pool)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, client.close)

        try:
            await client.run(handle)
        finally:
            exporter.cancel()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
