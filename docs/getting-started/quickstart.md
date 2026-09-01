# Quickstart

Get walbox running in 5 minutes: write rows to an outbox table, see them delivered to your handler with a durable checkpoint.

## 1. Install

```sh
pip install walbox
```

By default, Psycopg uses your system's libpq. If you don't have libpq installed:

```sh
pip install walbox psycopg[binary]
```

## 2. Configure PostgreSQL

Ensure your PostgreSQL instance has logical replication enabled. Add this to `postgresql.conf`:

```
wal_level = logical
```

Then restart PostgreSQL and run these commands once (as a superuser or role with `REPLICATION` privilege):

```sql
CREATE TABLE outbox (
    id          BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE PUBLICATION walbox_pub FOR TABLE outbox;
```

See [Setup & Deployment](../production/setup.md) for production configuration, managed PostgreSQL details, and exact privilege requirements.

## 3. Create the handler

Create `handler.py`:

```python
import asyncio
import signal
from walbox import (
    ChangeKind,
    CheckpointHandle,
    Transaction,
    Walbox,
    WalboxOptions,
)


async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue
        print(f"Received: {change.new}")

    await checkpoint.save(tx.commit_lsn)


async def main() -> None:
    options = WalboxOptions(
        consumer_name="my-consumer",
        dsn="postgresql://user:password@localhost/dbname",
        slot_name="outbox_slot",
        publication_name="walbox_pub",
    )

    client = Walbox.build(options)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, client.close)

    await client.run(handle)


if __name__ == "__main__":
    asyncio.run(main())
```

Replace the PostgreSQL connection string with your own.

## 4. Run the handler

```sh
python handler.py
```

You'll see log output indicating the replication client started.

## 5. Insert an event

In another terminal, insert a row:

```sql
INSERT INTO outbox (entity_type, entity_id, event_type, payload)
VALUES ('user', '42', 'created', '{"name": "Alice"}'::jsonb);
```

## 6. See the result

Back in the handler terminal, you'll see:

```
Received: {'id': 1, 'entity_type': 'user', 'entity_id': '42', 'event_type': 'created', 'payload': {'name': 'Alice'}, 'created_at': ...}
```

## What just happened

```mermaid
graph LR
    A["INSERT<br/>to outbox"] --> B["PostgreSQL<br/>WAL"]
    B --> C["walbox<br/>reads"]
    C --> D["Handler<br/>processes"]
    D --> E["Checkpoint<br/>saved"]
```

1. The INSERT wrote to PostgreSQL's transaction log
2. walbox received it via logical replication
3. Your handler printed the row
4. walbox saved a checkpoint recording the progress
5. On restart, walbox resumes from the last durable checkpoint. This provides at-least-once delivery for committed outbox events.

## Next steps

- **Learn more**: [Architecture](../production/architecture.md) explains the full model and [Delivery Guarantees](../production/delivery-guarantees.md) covers what at-least-once means
- **Production setup**: See [Setup & Deployment](../production/setup.md) for PostgreSQL configuration, managed databases, and running in production
- **Examples**: Browse [working examples](../examples/transactional-outbox.md) for the outbox and [PostgreSQL sink](../examples/postgresql.md) patterns, or the runnable scripts in [`examples/`](https://github.com/mochams/walbox/tree/main/examples) for message brokers, sharded concurrency, and metrics
