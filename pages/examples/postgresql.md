# PostgreSQL Sink

This example shows the same-transaction checkpoint pattern when your sink is PostgreSQL itself. This achieves exactly-once effects without external deduplication.

The code is in [`examples/outbox_postgres.py`](https://github.com/mochams/walbox/blob/main/examples/outbox_postgres.py) and [`examples/outbox_postgres_pool.py`](https://github.com/mochams/walbox/blob/main/examples/outbox_postgres_pool.py).

## The pattern

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        # Write your application state
        for change in tx.changes:
            if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
                continue

            await conn.execute(
                "INSERT INTO events (...) VALUES (...)",
                (change.new["entity_type"], ...)
            )

        # Save checkpoint in the SAME transaction
        await checkpoint.save(tx.commit_lsn, connection=conn)

        # Commit both together
        await conn.commit()
```

The key difference from the [Transactional Outbox](transactional-outbox.md) example: you pass `connection=conn` to `checkpoint.save()`, which means it updates the checkpoint row in the same transaction as your application writes. Either both commit or both rollback.

## Setup

Create both the outbox table and your sink table:

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

-- Your sink table (whatever you're projecting into)
CREATE TABLE events (
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

## Run the example

```bash
export WALBOX_DSN="postgresql://user:password@localhost/dbname"
python examples/outbox_postgres.py
```

In another terminal, insert a row:

```sql
INSERT INTO outbox (entity_type, entity_id, event_type, payload)
VALUES ('user', '42', 'created', '{"name": "Alice"}'::jsonb);
```

The handler writes to the `events` table and saves the checkpoint in one transaction.

## Why this is exactly-once

If the process crashes at any point:

1. **Before application write**: Checkpoint wasn't saved, so the transaction is redelivered on restart.
2. **Before checkpoint save**: Same as above.
3. **Before commit**: Both the application write and checkpoint update are uncommitted, so they rollback.
4. **After commit**: Both are durable, and the next restart resumes from the new checkpoint.

There's no scenario where the application write succeeds but the checkpoint doesn't (or vice versa), because they're in the same transaction.

## With connection pooling

To use a connection pool (more efficient for high-volume workloads):

```python
from psycopg_pool import AsyncConnectionPool

async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    async with pool.connection() as conn:
        for change in tx.changes:
            if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
                continue

            await conn.execute("INSERT INTO events (...) VALUES (...)", ...)

        await checkpoint.save(tx.commit_lsn, connection=conn)
        # No explicit commit; the context manager commits

# In main()
async with AsyncConnectionPool(dsn, min_size=5, max_size=10) as pool:
    checkpoint_store = PostgresCheckpointStore.from_pool(pool, consumer_name="...")
    # ... rest of setup
    await client.run(handle)
```

The `from_pool()` checkpoint store uses connections from your pool for checkpoint operations, while your handler can use the same pool for application writes.

## No deduplication needed

Because the application write and checkpoint are atomic, you don't need deduplication on the sink side. Each transaction is delivered exactly once to your sink table.

## When to use this pattern

Use the same-transaction pattern when:

- Your sink is PostgreSQL (or any transactional database)
- You need exactly-once effects without deduplication
- You want to ensure consistency between your outbox and your sink

For external brokers (Kafka, RabbitMQ, HTTP webhooks), use the [Transactional Outbox](transactional-outbox.md) pattern with broker-side deduplication.

## Performance considerations

Writing to PostgreSQL is usually fast, but the same-transaction pattern adds one database write per batch. For high-volume workloads (thousands of transactions per second), this can add latency.

Profile your handler to see if this is a bottleneck. If it is, consider:

1. Batching multiple transactions into one checkpoint commit (walbox already does this for you)
2. Using connection pooling (as shown above) to amortize connection overhead
3. Optimizing your application writes (indexes, prepared statements, etc.)

## Summary

- Same-transaction checkpoint gives you exactly-once effects without external deduplication
- Works only when your sink is a database that can participate in a transaction
- More complex to set up than basic outbox, but guarantees atomic writes
- Use connection pooling for better performance at scale
