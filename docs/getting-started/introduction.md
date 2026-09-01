# Introduction

walbox is an async Python runtime for consuming PostgreSQL logical replication as a stream of committed transactions.

## What walbox is

- **A PostgreSQL change stream**: It reads logical replication directly from PostgreSQL, so your app can react to committed changes without polling.
- **Transaction-aware**: It delivers row changes grouped by committed transaction, making downstream processing easier to reason about.
- **Resumable**: Durable checkpoints let walbox resume from the last safe position after a crash, reconnect, or restart.
- **Backpressure-aware**: A bounded queue prevents slow handlers from causing unbounded memory growth.
- **Handles reconnects and shutdowns**: It reconnects when needed and waits for in-flight work to finish during shutdown.
- **Works with any published table**: walbox can consume changes from any table included in your PostgreSQL publication.
- **Async Python**: It is built on asyncio and uses `psycopg3`.

## Know the limits

Before you adopt walbox, make sure these fit your needs:

- **Not a general database replication system**: it doesn't copy schema, handle DDL, replicate full tables, or track column changes. It reads the tables you publish and delivers their rows.
- **Not a CDC/ETL platform**: if you need general change data capture, historical extraction, or a data pipeline, use a dedicated platform instead.
- **Not a message broker**: it doesn't offer topics, subscriptions, consumer groups, or a persistent message store.
- **Not horizontally scalable by itself**: a single consumer processes its replication stream sequentially. For parallelism, run multiple independent consumers, each with its own replication slot. See [Setup & Deployment](../production/setup.md#multi-consumer-deployments) for multi-consumer setup.
- **Checkpointing is always manual**: your handler calls `checkpoint.save()`; walbox never checkpoints on its own. See [Manual checkpointing](../production/delivery-guarantees.md#manual-checkpointing).
- **Delivery is at-least-once, not exactly-once**: your handler may see the same transaction more than once, and your application needs to tolerate that. See [Delivery Guarantees](../production/delivery-guarantees.md).
- **A handler exception ends the process**: walbox doesn't catch it or skip the transaction silently. See [Handler failure behavior](../production/delivery-guarantees.md#handler-failure-behavior).
- **PostgreSQL is the only checkpoint backend**: there's no file-based or in-memory option. See [Checkpoint stores](../production/delivery-guarantees.md#checkpoint-stores).
- **Large transactions are buffered in memory during assembly**, with no configurable limit today. See [Large transactions](../production/monitoring.md#large-transactions).
- **Requires logical replication already enabled** on your PostgreSQL instance (`wal_level = logical`). See [Setup & Deployment](../production/setup.md).

## Next steps

- **New to walbox?** Start with [Quickstart](quickstart.md).
- **Planning a production deployment?** See the [Production Guide](../production/architecture.md).
- **Looking for working examples?** Check out the [Examples section](../examples/transactional-outbox.md).
