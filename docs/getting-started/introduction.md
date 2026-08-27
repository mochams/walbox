# Introduction

walbox is an async Python runtime for consuming PostgreSQL logical replication as a stream of committed transactions.

## What walbox is

- **At-least-once delivery**: a durable local checkpoint ensures no silent loss
- **Bounded delivery queue**: prevents unbounded backlogs from accumulating in memory when handlers fall behind
- **Automatic reconnection and recovery**: picks up from the last durable checkpoint if the process restarts
- **Graceful shutdown**: lets in-flight work complete before exiting, given sufficient termination time
- **Asyncio-native**: one Python dependency (`psycopg`)

## Know the limits

walbox is built for a specific pattern: one Python process consuming a single PostgreSQL replication slot and writing committed transactions to an external system. Before you adopt it, make sure it fits your needs. It is not:

- **A general database replication system**: it doesn't copy schema, handle DDL, replicate full tables, or track column changes. It reads the tables you publish and delivers their rows.
- **A CDC/ETL platform**: if you need general change data capture, historical extraction, or a data pipeline, use a dedicated platform like Kafka Connect or Debezium instead.
- **A message broker**: it doesn't offer topics, subscriptions, consumer groups, or a persistent message store. It delivers transactions to one application handler, in order.
- **A horizontal scaling tool by itself**: a single consumer processes its replication stream sequentially. For parallelism, run multiple independent consumers, each with its own replication slot and a disjoint subset of data through application-level partitioning. See [Setup & Deployment](../production/setup.md) for multi-consumer setup.

## Next steps

- **New to walbox?** Start with [Quickstart](quickstart.md).
- **Planning a production deployment?** See the [Production Guide](../production/architecture.md).
- **Looking for working examples?** Check out the [Examples section](../examples/transactional-outbox.md).
