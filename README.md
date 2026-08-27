# walbox

[![PyPI](https://img.shields.io/pypi/v/walbox.svg)](https://pypi.org/project/walbox/)
[![CI](https://github.com/mochams/walbox/actions/workflows/ci.yml/badge.svg)](https://github.com/mochams/walbox/actions/workflows/ci.yml)
[![Python 3.13+](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Async Python runtime for consuming PostgreSQL logical replication as a stream of committed transactions. Built for the **transactional outbox pattern**: write an outbox row in the same database transaction as your business data, then stream those committed inserts to an external system with no polling and no `LISTEN`/`NOTIFY`.

**[📖 Documentation](https://mochams.github.io/walbox/) · [⚡ Quickstart](https://mochams.github.io/walbox/getting-started/quickstart/) · [🏗️ Architecture](ARCHITECTURE.md)**

## The problem

Applications often need to update their database and publish an event to an external system as one logical operation. Doing both directly creates a dual-write problem: the database transaction can commit while the external publish fails, or the publish can succeed while the application crashes before recording that it was delivered.

## The solution

The **transactional outbox pattern** solves this by writing the business data and an outbox event in the same PostgreSQL transaction. The event is then delivered asynchronously to the external system.

walbox takes the pattern a step further. It consumes committed outbox changes directly from PostgreSQL logical replication, so there is no polling loop and no `LISTEN/NOTIFY` coordination. Events become available from the database WAL as transactions commit, while durable checkpoints provide recovery and at-least-once delivery.

## Guarantees

- **At-least-once delivery**: a durable local checkpoint ensures committed events are not silently skipped
- **Transactional consistency**: outbox events are committed atomically with your business data
- **No polling**: consumes changes directly from PostgreSQL logical replication
- **Bounded backpressure**: queue limits prevent a slow handler from accumulating unbounded memory
- **Automatic recovery**: resumes from the last durable checkpoint on restart
- **Graceful shutdown**: lets in-flight work complete before exiting
- **Asyncio-native**: built for Python's async runtime

## Install

```sh
pip install walbox
```

Psycopg 3 is the only Python dependency. By default it uses your system's `libpq`; if you don't have it installed, use the binary distribution: `pip install walbox psycopg[binary]`.

## Example

PostgreSQL setup (one-time):

```sql
CREATE TABLE outbox (
    id BIGSERIAL PRIMARY KEY, entity_type TEXT, entity_id TEXT,
    event_type TEXT, payload JSONB, created_at TIMESTAMPTZ DEFAULT now()
);
CREATE PUBLICATION walbox_pub FOR TABLE outbox;
```

Handler (`handler.py`):

```python
import asyncio
from walbox import (
    ChangeKind,
    CheckpointHandle,
    PostgresCheckpointStore,
    ReplicationClient,
    ReplicationOptions,
    Transaction,
)


async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    for change in tx.changes:
        if change.table == "public.outbox" and change.kind == ChangeKind.INSERT:
            print(f"Event: {change.new}")
    await checkpoint.save(tx.commit_lsn)


async def main():
    dsn = "postgresql://user:password@localhost/db"
    checkpoint_store = PostgresCheckpointStore(dsn, consumer_name="app")
    client = ReplicationClient(
        ReplicationOptions(
            consumer_name="app",
            dsn=dsn,
            slot_name="slot",
            publication_name="walbox_pub",
            checkpoint_store=checkpoint_store,
        )
    )
    await client.run(handle)


asyncio.run(main())
```

Run it, then insert a row:

```sh
python handler.py
# In another terminal:
INSERT INTO outbox (entity_type, entity_id, event_type, payload)
VALUES ('user', '42', 'created', '{"name":"Alice"}'::jsonb);
```

The handler receives the row and saves a durable checkpoint. On restart, it resumes from that checkpoint—no data loss.

See [**Examples**](https://github.com/mochams/walbox/tree/main/examples) for working patterns: webhooks, message brokers, PostgreSQL sinks, and more.

## Next steps

- **[Quickstart](https://mochams.github.io/walbox/getting-started/quickstart/)** (5 minutes): step-by-step setup
- **[Getting Started](https://mochams.github.io/walbox/getting-started/introduction/)**: concepts and guarantees
- **[Production Guide](https://mochams.github.io/walbox/production/architecture/)**: deployment, monitoring, configuration

## Status

walbox is still in its early days. It is tested with 100% branch coverage and integration tests against real PostgreSQL, but the API may change as the project evolves toward 1.0.0.

**Development**: see [`CONTRIBUTING.md`](CONTRIBUTING.md) for setup and [`LICENSE`](LICENSE) for terms.
