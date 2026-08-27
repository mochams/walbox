# Introduction

walbox is an async Python runtime for consuming PostgreSQL logical replication as a stream of committed transactions. It's built for the **transactional outbox pattern**: write an outbox row in the same database transaction as your business data, then stream those committed inserts to an external system with no polling and no `LISTEN`/`NOTIFY`.

## What walbox is

- **At-least-once delivery**: a durable local checkpoint ensures no silent loss
- **Bounded delivery queue**: prevents unbounded backlogs from accumulating in memory when handlers fall behind
- **Automatic reconnection and recovery**: picks up from the last durable checkpoint if the process restarts
- **Graceful shutdown**: lets in-flight work complete before exiting, given sufficient termination time
- **Asyncio-native**: one Python dependency (`psycopg`)

## When it's the wrong tool

walbox is built for a specific pattern: one Python process consuming a single PostgreSQL replication slot and writing committed transactions to an external system. Before you adopt it, make sure it fits your needs.

**Single-consumer design**: A walbox consumer processes its replication stream sequentially. For horizontal parallelism, run multiple independent consumers, each with its own replication slot and a disjoint subset of data (via application-level partitioning). See [Deployment](../production/deployment.md) for multi-consumer setup and [`examples/outbox_concurrency.py`](https://github.com/mochams/walbox/blob/main/examples/outbox_concurrency.py) for scaling handler work within a single consumer using sharded queues.

**PostgreSQL access**: walbox needs `REPLICATION` privilege on the connecting role. This may not be available on managed PostgreSQL tiers; check your provider's documentation. See [PostgreSQL Setup](../production/setup.md) for exact privilege requirements.

**Not a full replication system**: walbox is optimized for the outbox pattern, not for table replication, schema migration, backfilling, or DDL propagation. If you need general CDC, consider a dedicated platform like Kafka Connect or Debezium.

## Next steps

- **New to walbox?** Start with [Quickstart](quickstart.md).
- **Planning a production deployment?** See the [Production Guide](../production/architecture.md).
- **Looking for working examples?** Check out the [Examples section](../examples/transactional-outbox.md).
