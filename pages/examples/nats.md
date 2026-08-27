# NATS Publish Example

This is an illustrative example showing how to publish to NATS JetStream. Unlike the [Transactional Outbox](transactional-outbox.md) and [PostgreSQL](postgresql.md) examples, **this code is not backed by a tested script in the repo**. It's provided as a reference implementation pattern; adjust it to your NATS client library and schema.

## The pattern

```python
import nats
from walbox import ChangeKind, CheckpointHandle, Transaction

# Initialize NATS connection once in main()
nc = None

async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue

        row = change.new
        # Publish to NATS with idempotency key for deduplication
        await nc.jetstream().publish(
            "outbox-events",
            json.dumps(row).encode(),
            # Use outbox.id as idempotent message ID in NATS
            headers=nats.msg.Headers({"nats-msg-id": f"outbox-{row['id']}"})
        )

    await checkpoint.save(tx.commit_lsn)

async def main() -> None:
    global nc
    nc = await nats.connect("nats://localhost:4222")
    js = nc.jetstream()

    # Ensure the stream exists
    try:
        await js.stream_info("outbox-stream")
    except nats.errors.NotFound:
        await js.add_stream(
            name="outbox-stream",
            subjects=["outbox-events"]
        )

    # ... walbox setup ...
    await client.run(handle)
```

## Key points

**Idempotency**: NATS JetStream's idempotent message ID prevents duplicates within a deduplication window (typically 2 minutes by default). Use `outbox.id` as the message ID to ensure each outbox row is delivered at most once within that window.

**Deduplication window**: If you're concerned about redelivery after a crash outside the deduplication window, implement consumer-side deduplication with a set or database table tracking seen IDs.

**Headers**: The `nats-msg-id` header (or your NATS library's equivalent) enables idempotent publishing.

## Full example (outline)

```python
import asyncio
import json
import logging
import nats
from walbox import (
    ChangeKind,
    CheckpointHandle,
    PostgresCheckpointStore,
    ReplicationClient,
    ReplicationOptions,
    Transaction,
)

logger = logging.getLogger("nats_example")

async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue

        row = change.new
        await nc.jetstream().publish(
            "outbox-events",
            json.dumps(row).encode(),
            headers={"nats-msg-id": f"outbox-{row['id']}"}
        )
        logger.info(f"Published outbox row {row['id']} to NATS")

    await checkpoint.save(tx.commit_lsn)

async def main() -> None:
    global nc
    nc = await nats.connect("nats://localhost:4222")

    checkpoint_store = PostgresCheckpointStore(dsn, consumer_name="nats-consumer")
    options = ReplicationOptions(
        consumer_name="nats-consumer",
        dsn=dsn,
        slot_name="outbox_slot",
        publication_name="walbox_pub",
        checkpoint_store=checkpoint_store,
    )

    client = ReplicationClient(options)
    # ... signal handlers ...
    await client.run(handle)

if __name__ == "__main__":
    asyncio.run(main())
```

## Dependencies

```bash
pip install walbox nats-py
```

## See also

- [Transactional Outbox](transactional-outbox.md) for the external broker pattern and deduplication strategy
- [Delivery Guarantees](../production/delivery-guarantees.md) for at-least-once semantics and when redelivery occurs
- [NATS documentation](https://docs.nats.io/) for JetStream configuration and consumer options
