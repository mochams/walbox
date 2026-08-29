# walbox

[![PyPI](https://img.shields.io/pypi/v/walbox.svg)](https://pypi.org/project/walbox/)
[![CI](https://github.com/mochams/walbox/actions/workflows/ci.yml/badge.svg)](https://github.com/mochams/walbox/actions/workflows/ci.yml)
[![Python 3.13+](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Async Python runtime for consuming PostgreSQL logical replication as a stream of committed transactions. Built for the **transactional outbox pattern**: write an outbox row in the same database transaction as your business data, then stream those committed inserts to an external system with no polling and no `LISTEN`/`NOTIFY`. That's the common case. The same guarantees apply to any table you publish.

**[📖 Documentation](https://mochams.github.io/walbox/) · [⚡ Quickstart](https://mochams.github.io/walbox/getting-started/quickstart/)**

## Some reasons you might want to use walbox

- **You're using the transactional outbox pattern** and need a reliable way to consume outbox rows.
- **You need to reliably tell another system about a change**, without polling a table or wiring up `LISTEN`/`NOTIFY` across processes.
- **You want to avoid the dual-write problem** between your database and an external system.
- **You want to use PostgreSQL's durability** without running a separate CDC platform for a simple use case.
- **You need to stream changes from other tables too.** walbox works with any table covered by your publication.

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
    Transaction,
    Walbox,
    WalboxOptions,
)


async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    for change in tx.changes:
        if change.table == "public.outbox" and change.kind == ChangeKind.INSERT:
            print(f"Event: {change.new}")
    await checkpoint.save(tx.commit_lsn)


async def main():
    options = WalboxOptions(
        consumer_name="app",
        dsn="postgresql://user:password@localhost/db",
        slot_name="slot",
        publication_name="walbox_pub",
    )
    client = Walbox.build(options)
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

The handler receives the row and saves a durable checkpoint. On restart, it resumes from that checkpoint. No data loss.

See [**Examples**](https://github.com/mochams/walbox/tree/main/examples) for working patterns: webhooks, message brokers, PostgreSQL sinks, and more.

## Next steps

- **[Quickstart](https://mochams.github.io/walbox/getting-started/quickstart/)** (5 minutes): step-by-step setup
- **[Getting Started](https://mochams.github.io/walbox/getting-started/introduction/)**: concepts and guarantees
- **[Production Guide](https://mochams.github.io/walbox/production/architecture/)**: deployment, monitoring, configuration

## Status

walbox is still in its early days. It is tested with 100% branch coverage and integration tests against real PostgreSQL, but the API may change as the project evolves toward 1.0.0.

See [`CHANGELOG.md`](CHANGELOG.md) for what's changed release to release.

**Development**: see [`CONTRIBUTING.md`](CONTRIBUTING.md) for setup and [`LICENSE`](LICENSE) for terms.
