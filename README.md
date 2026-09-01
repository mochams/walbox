# walbox

[![PyPI](https://img.shields.io/pypi/v/walbox.svg)](https://pypi.org/project/walbox/)
[![CI](https://github.com/mochams/walbox/actions/workflows/ci.yml/badge.svg)](https://github.com/mochams/walbox/actions/workflows/ci.yml)
[![Python 3.13+](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Async Python runtime for consuming PostgreSQL logical replication as a stream of committed transactions.

**[📖 Documentation](https://mochams.github.io/walbox/) · [⚡ Quickstart](https://mochams.github.io/walbox/getting-started/quickstart/)**

**Some reasons you might want to use walbox:**

- **You want to react to database changes in real time** without polling tables or adding fragile trigger-based workarounds.
- **You care about reliable delivery**: walbox checkpoints progress durably so it can resume cleanly after restarts or connection drops.
- **You’re building around the transactional outbox pattern** and want a consumer that preserves commit ordering and handles recovery sensibly.
- **You want backpressure built in**, so a slow handler does not turn into unbounded memory growth or silent data loss.
- **You need a Python async consumer** that plugs directly into PostgreSQL logical replication instead of shoving data through a separate broker.
- **You want to stream changes from published tables** without writing your own replication client, reconnect logic, or checkpoint tracking.
- **You want operational visibility**: retries, lag, queue depth, and checkpoint timing are all surfaced so production behavior is easier to reason about.

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

See [**Examples**](https://github.com/mochams/walbox/tree/main/examples) for working patterns: publishing to a message broker, writing to a PostgreSQL sink with exactly-once effects, sharded concurrent handling, and metrics.

## Next steps

- **[Quickstart](https://mochams.github.io/walbox/getting-started/quickstart/)** (5 minutes): step-by-step setup
- **[Getting Started](https://mochams.github.io/walbox/getting-started/introduction/)**: concepts and guarantees
- **[Production Guide](https://mochams.github.io/walbox/production/architecture/)**: deployment, monitoring, configuration

## Status

walbox is at v1.0.0. It has 100% branch coverage, integration tests against real PostgreSQL, and went through three pre-release cycles (beta.1, beta.2, rc.1) to settle the API.

See [`CHANGELOG.md`](CHANGELOG.md) for what's changed release to release, and the [Upgrading guide](https://mochams.github.io/walbox/migration/) if you're on a pre-1.0.0 version.

**Development**: see [`CONTRIBUTING.md`](CONTRIBUTING.md) for setup and [`LICENSE`](LICENSE) for terms.
