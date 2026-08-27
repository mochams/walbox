# Transactional Outbox

This example shows the fundamental walbox pattern: consume outbox rows from PostgreSQL and publish them to an external broker.

The code is in [`examples/outbox.py`](https://github.com/mochams/walbox/blob/main/examples/outbox.py).

## The pattern

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue

        # Publish to your external broker
        await publish_to_broker(change.new)

    # Save the checkpoint durably
    await checkpoint.save(tx.commit_lsn)
```

1. For each transaction received from PostgreSQL
2. Iterate over the changes (INSERT, UPDATE, DELETE)
3. Filter to only INSERT events on the outbox table
4. Publish each row to your external broker
5. Save the checkpoint durably

## Setup

Create the outbox table and publication:

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

## Run the example

```bash
export WALBOX_DSN="postgresql://user:password@localhost/dbname"
python examples/outbox.py
```

In another terminal, insert a row:

```sql
INSERT INTO outbox (entity_type, entity_id, event_type, payload)
VALUES ('user', '42', 'created', '{"name": "Alice"}'::jsonb);
```

The handler publishes it to the (simulated) broker and saves the checkpoint.

## Deduplication

Since walbox delivers at-least-once, your handler may be called with the same row twice if the process crashes and restarts. Your external broker must deduplicate using the `outbox.id` as the idempotency key:

```python
await broker.publish(
    key=f"outbox-{change.new['id']}",  # Idempotency key
    value=change.new
)
```

Most brokers (Kafka, RabbitMQ, etc.) support idempotent producers or offer an idempotency key mechanism.

## Checkpoint behavior

In this example, `checkpoint.save(tx.commit_lsn)` is called without a `connection=` argument, which means it opens its own connection to save the checkpoint to a separate row in the `walbox_checkpoints` table. This is appropriate because the external broker write can't be atomic with the checkpoint anyway (they're in different systems).

For exactly-once effects when the sink is PostgreSQL, see [PostgreSQL Example](postgresql.md).

## Key properties

- **Delivery guarantee**: At-least-once to the broker; exactly-once *effects* via broker-side deduplication
- **Scope**: One transaction at a time, delivered to your handler sequentially
- **Order**: Within walbox (single consumer), events are delivered in the order they were committed
- **Backpressure**: If the broker is slow, the handler backs up, which blocks walbox's receiver, which slows PostgreSQL

## Next steps

- For a PostgreSQL sink instead of an external broker, see [PostgreSQL Example](postgresql.md)
- For connection management details, see [Shutdown & Lifecycle](../production/shutdown-lifecycle.md)
- For deployment, see [Deployment Considerations](../production/deployment.md)
