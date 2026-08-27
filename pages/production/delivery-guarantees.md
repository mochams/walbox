# Delivery Guarantees

walbox provides **at-least-once delivery** with a durable checkpoint. This page explains what that means, when duplicates can occur, and how to achieve exactly-once effects in your application.

## The core guarantee

walbox guarantees:

```
handler successfully processes transaction
    ↓
checkpoint becomes durable
    ↓
replication feedback may advance
```

walbox never advances the durable replication feedback position beyond the application's durable processing boundary. If the process crashes at any point, replication resumes from the most recent durable checkpoint, and transactions since that checkpoint are redelivered.

## At-least-once vs. exactly-once effects

**At-least-once delivery** is what walbox provides. Your handler may be called with the same transaction more than once.

**Exactly-once effects** is what your application achieves. The application state ends up as if each transaction was processed exactly once, despite potential redelivery. This requires either:
- An **idempotent sink** (the handler can safely be retried)
- A **deduplicating sink** (external system tracks and deduplicates by event ID)
- An **atomic write + checkpoint** (application state and checkpoint written in one transaction)

## How the guarantee works

For each transaction, the sequence is:

1. **PostgreSQL commits** an outbox row in your database
2. **walbox receives it** via logical replication
3. **walbox delivers it** to your handler
4. **Your handler runs** to completion
5. **Checkpoint is saved** (you call `checkpoint.save(tx.commit_lsn)`)
6. **Replication feedback is sent** to PostgreSQL

If the process crashes or exits at any point before step 5 completes, the checkpoint is not durable, and the transaction will be redelivered on restart.

If the process crashes after step 5 completes, the transaction will not be redelivered (the checkpoint is durable).

## When duplicates happen

Duplicates occur when the process crashes or exits between these points:

- **During or before handler execution** → handler hasn't completed, checkpoint not saved, transaction is redelivered
- **After handler completes, before checkpoint saved** → handler succeeded but checkpoint is lost, transaction is redelivered (canonical at-least-once case)

Durability is never sacrificed to avoid duplicates. This is a deliberate design choice: it's better to replay a transaction than to lose it.

**Important**: duplicates are an expected consequence of at-least-once delivery. Your application should be prepared to handle them, either through idempotency or deduplication.

## Checkpoint stores and durability

### FileCheckpointStore

Saves the checkpoint to a JSON file on disk using atomic rename:

```python
checkpoint_store = FileCheckpointStore("./checkpoint.json")
```

When you call `checkpoint.save(lsn)` without a `connection=` argument, walbox writes to a temporary file and renames it atomically. This is durable once the rename completes.

### PostgresCheckpointStore

Saves the checkpoint as a row in your PostgreSQL database:

```python
checkpoint_store = PostgresCheckpointStore(dsn, consumer_name="my-consumer")
```

**Without a connection** (`checkpoint.save(lsn)`):
- walbox opens its own connection
- executes an INSERT or UPDATE to the `walbox_checkpoints` table
- commits immediately
- the checkpoint is durable once the commit completes

**With a caller's connection** (`checkpoint.save(lsn, connection=conn)`):
- walbox executes the INSERT or UPDATE on your connection
- **does not commit** (you must commit)
- the checkpoint participates in your transaction
- the checkpoint is durable only when your transaction commits

## Exactly-once effects: external brokers

For external systems (Kafka, RabbitMQ, webhooks), use an idempotency key:

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue

        # Send with a stable idempotency key
        await broker.publish(
            key=f"outbox-{change.new['id']}",
            value=change.new
        )

    await checkpoint.save(tx.commit_lsn)
```

The pattern:

1. Use the `outbox.id` as your idempotency key
2. Pass it to the external system (as a Kafka key, message ID, Idempotency-Key header, etc.)
3. The external system must support idempotent ingestion:
   - Some systems (Kafka with idempotent producer, many HTTP APIs) do this automatically
   - Others require you to implement consumer-side deduplication
   - Read your broker's documentation for its idempotency semantics

If walbox redelivers the transaction, the external system sees the same key again and either deduplicates or rejects the duplicate (depending on its semantics).

Note: not every external system will deduplicate automatically just because an idempotency key is supplied. For example, an HTTP webhook endpoint does not automatically become idempotent—the receiving application must implement idempotency handling.

## Exactly-once effects: PostgreSQL sink

When your sink is PostgreSQL itself, use the **same-transaction pattern**:

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    async with pool.connection() as conn:
        async with conn.transaction():
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
            # conn.transaction() will commit here
```

**Why this works**: the application state write and the checkpoint update both participate in the same PostgreSQL transaction. Either both commit or both roll back:

```
handler executes
    ↓
write application state
    ↓
save checkpoint (on caller's connection, uncommitted)
    ↓
transaction commits
   ↙  ↘
success   failure
  ↓        ↓
both     neither
durable  durable
  ↓
continue  redelivered
```

If the process crashes before the COMMIT, both the application state and the checkpoint are rolled back. On restart, walbox resumes from the previous checkpoint, and the transaction is redelivered.

This guarantees exactly-once effects without external deduplication.

## Checkpoint and replication feedback

walbox maintains two pieces of state:

- **Your durable checkpoint**: the LSN up to which you've durably processed
- **Replication feedback**: what walbox tells PostgreSQL about its progress

**Important distinction**: Your durable checkpoint is the application's source of truth. Replication feedback is a hint to PostgreSQL for WAL retention and slot management.

The feedback is sent after the checkpoint is saved, but it can lag behind. If the process crashes between checkpoint persistence and feedback, PostgreSQL may resend transactions you've already checkpointed. This is safe (your handler should be idempotent anyway).

Do not rely on PostgreSQL's feedback position as your own checkpoint. Always checkpoint in your application's own storage (FileCheckpointStore or PostgresCheckpointStore).

## What if things go wrong?

**Handler crashes**: The transaction is not checkpointed. On restart, it's redelivered.

**Checkpoint save fails**: The checkpoint doesn't become durable. On restart, it's redelivered.

**Process crashes between checkpoint and feedback**: The transaction was checkpointed durably, but PostgreSQL wasn't told. On restart, walbox resumes from the checkpoint—no redelivery. PostgreSQL might also resend if its own confirmed position lagged.

**PostgreSQL restarts**: Depends on whether your checkpoint outlasted the outage. If your checkpoint was persisted (to FileCheckpointStore or PostgreSQL), walbox resumes from there. Transactions after that checkpoint are redelivered.

For detailed crash-point analysis, see [Checkpointing & Recovery](checkpointing-recovery.md).

## Summary

- **walbox guarantees at-least-once delivery**, not exactly-once
- **Duplicates are expected**—your handler must tolerate redelivery
- **Durability comes first**—a replay is preferable to losing a committed event
- **External brokers** require idempotent ingestion or consumer-side deduplication
- **PostgreSQL sinks** can be made exactly-once via same-transaction atomicity
- **Your checkpoint is the source of truth**, not replication feedback
