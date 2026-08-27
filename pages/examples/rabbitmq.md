# RabbitMQ Publish Example

This is an illustrative example showing how to publish to RabbitMQ. Unlike the [Transactional Outbox](transactional-outbox.md) and [PostgreSQL](postgresql.md) examples, **this code is not backed by a tested script in the repo**. It's provided as a reference implementation pattern; adjust it to your RabbitMQ client library and schema.

## The pattern

```python
import aio_pika
from walbox import ChangeKind, CheckpointHandle, Transaction

connection = None
channel = None

async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue

        row = change.new
        # Publish to RabbitMQ with confirmation
        exchange = await channel.get_exchange("outbox-events")
        message = aio_pika.Message(
            body=json.dumps(row).encode(),
            # Use outbox.id for deduplication
            message_id=f"outbox-{row['id']}",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        await exchange.publish(message, routing_key="events")

    await checkpoint.save(tx.commit_lsn)

async def main() -> None:
    global connection, channel
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()

    # Declare exchange and queue
    exchange = await channel.declare_exchange("outbox-events", aio_pika.ExchangeType.TOPIC)
    queue = await channel.declare_queue("outbox-queue")
    await queue.bind(exchange, "events")

    # ... walbox setup ...
    await client.run(handle)
```

## Key points

**Persistence**: Set `delivery_mode=aio_pika.DeliveryMode.PERSISTENT` so messages survive broker restarts.

**Idempotency**: RabbitMQ doesn't natively support idempotent message IDs, so you need consumer-side deduplication. Use the `message_id` field to tag each message with the `outbox.id`, then deduplicate on the consumer side by tracking seen IDs.

**Publisher confirms**: Some RabbitMQ clients offer publisher confirms (acknowledgments). If your client supports it, wait for confirmation before calling `checkpoint.save()`:

```python
await exchange.publish(message, routing_key="events").wait()
```

This ensures the message was received by the broker before you checkpoint.

## Full example (outline)

```python
import asyncio
import json
import logging
import aio_pika
from walbox import (
    ChangeKind,
    CheckpointHandle,
    PostgresCheckpointStore,
    ReplicationClient,
    ReplicationOptions,
    Transaction,
)

logger = logging.getLogger("rabbitmq_example")

connection = None
channel = None

async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue

        row = change.new
        exchange = await channel.get_exchange("outbox-events")

        message = aio_pika.Message(
            body=json.dumps(row).encode(),
            message_id=f"outbox-{row['id']}",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        await exchange.publish(message, routing_key="events")
        logger.info(f"Published outbox row {row['id']} to RabbitMQ")

    await checkpoint.save(tx.commit_lsn)

async def main() -> None:
    global connection, channel
    connection = await aio_pika.connect_robust("amqp://guest:guest@localhost/")
    channel = await connection.channel()

    exchange = await channel.declare_exchange("outbox-events", aio_pika.ExchangeType.TOPIC)
    queue = await channel.declare_queue("outbox-queue")
    await queue.bind(exchange, "events")

    checkpoint_store = PostgresCheckpointStore(dsn, consumer_name="rabbitmq-consumer")
    options = ReplicationOptions(
        consumer_name="rabbitmq-consumer",
        dsn=dsn,
        slot_name="outbox_slot",
        publication_name="walbox_pub",
        checkpoint_store=checkpoint_store,
    )

    replication_client = ReplicationClient(options)
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, replication_client.close)

    await replication_client.run(handle)

if __name__ == "__main__":
    asyncio.run(main())
```

## Dependencies

```bash
pip install walbox aio-pika
```

## Consumer-side deduplication

On the consuming side, track which `outbox.id` values you've already processed:

```python
seen_ids = set()

async def process_message(message: aio_pika.IncomingMessage) -> None:
    message_id = message.message_id

    if message_id in seen_ids:
        await message.ack()
        return

    # Process the message
    data = json.loads(message.body)
    await handle_event(data)

    seen_ids.add(message_id)
    await message.ack()
```

For persistence across restarts, use a database table to track seen IDs instead of an in-memory set.

## See also

- [Transactional Outbox](transactional-outbox.md) for the external broker pattern and deduplication strategy
- [Delivery Guarantees](../production/delivery-guarantees.md) for at-least-once semantics
- [aio-pika documentation](https://aio-pika.readthedocs.io/) for exchange and queue configuration
