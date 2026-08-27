# Checkpointing & Recovery

A checkpoint records the replication position that your application has successfully processed and chosen to persist. When walbox restarts, it resumes from the last saved checkpoint.

## How checkpointing works

The flow for each transaction is:

1. **PostgreSQL commits** an outbox row
2. **walbox receives it** via logical replication
3. **walbox delivers it** to your handler
4. **Your handler runs** and completes successfully
5. **Checkpoint is persisted** (via `checkpoint.save(tx.commit_lsn)`)
6. **Replication progress advances** based on the durable checkpoint

Only after step 5 completes can step 6 happen. If the process crashes at any point before step 5, the checkpoint does not advance, and the transaction is redelivered on restart.

## Manual checkpointing

**Important: checkpointing is explicit and manual.** Your handler must call `checkpoint.save(tx.commit_lsn)` itself. walbox never checkpoints automatically because it cannot know whether your external side effect has become durable.

This is intentional: walbox is not responsible for the durability of your sink. If you send a message to a broker or HTTP endpoint, you must verify success before saving the checkpoint. The correct pattern is:

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    # 1. Send side effect
    await broker.publish(message)  # or equivalent

    # 2. Only after verifying success, save checkpoint
    await checkpoint.save(tx.commit_lsn)
```

**Incorrect pattern** (will lose data on crash):

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    # WRONG: checkpoint saved before side effect is confirmed
    await checkpoint.save(tx.commit_lsn)

    # If this crashes, the checkpoint advances but the side effect may not be durable
    await broker.publish(message)
```

A crash between checkpoint save and side effect durability will result in lost events because the checkpoint position has advanced past them on restart.

## Checkpoint storage options

### FileCheckpointStore

Stores the checkpoint as a JSON file on disk:

```python
from walbox import FileCheckpointStore

checkpoint_store = FileCheckpointStore("./checkpoint.json")
```

On startup, walbox reads the checkpoint file. On each handler completion, it writes the new LSN to disk atomically via temporary file + rename.

**When to use**: External sinks (Kafka, HTTP, etc.) where you can't share a transaction with the checkpoint.

**Tradeoff**: The checkpoint and your external side effect are not committed atomically. If walbox crashes after the side effect succeeds but before the checkpoint is durable, the transaction will be delivered again on restart. Your sink must be prepared to handle this via idempotency or deduplication.

### PostgresCheckpointStore

Stores the checkpoint in your PostgreSQL database:

```python
from walbox import PostgresCheckpointStore

checkpoint_store = PostgresCheckpointStore(dsn, consumer_name="my-consumer")
```

PostgresCheckpointStore creates a `walbox_checkpoints` table automatically. On startup, it loads the latest checkpoint for your consumer. On each call to `checkpoint.save(lsn)`, it updates the row atomically.

**When to use**: PostgreSQL sinks where you want exactly-once effects.

**Benefit**: You can save the checkpoint and your application data in the same transaction (the same-transaction pattern), achieving exactly-once effects without external deduplication.

## The same-transaction pattern

When your sink is PostgreSQL and you want exactly-once effects, save the checkpoint and your application state in one transaction:

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    async with pool.connection() as conn:
        async with conn.transaction():
            for change in tx.changes:
                if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
                    continue

                # Write your application state
                await conn.execute(
                    "INSERT INTO events (...) VALUES (...)",
                    ...
                )

            # Save checkpoint in the SAME transaction
            await checkpoint.save(tx.commit_lsn, connection=conn)
            # conn.transaction() auto-commits here
```

**How it works**: You pass `connection=conn` to `checkpoint.save()`, which makes it update the checkpoint row in the same transaction rather than opening its own connection. The application state write and the checkpoint update both participate in a single PostgreSQL transaction. Either both commit or both roll back:

```
handler executes
    ↓
write application state
    ↓
save checkpoint (on caller's connection, uncommitted)
    ↓
COMMIT
   ↙  ↘
success   failure
  ↓        ↓
both     neither durable
durable    ↓
  ↓      transaction
continue  redelivered
```

**Requirements for exactly-once effects**:

- Your application state and checkpoint are in the **same PostgreSQL database**
- Both use the **same transaction** (same connection, single COMMIT)
- The checkpoint is written using `checkpoint.save(lsn, connection=conn)`
- The transaction only commits after both the state write and checkpoint update succeed

If the process crashes before COMMIT, both the application state and checkpoint are rolled back. On restart, the transaction is redelivered and your handler runs again on the same data, but with no duplicate in the database.

This provides exactly-once effects for the PostgreSQL state change by making the sink write and checkpoint durability atomic.

## Pooled connections

For efficiency with a connection pool:

```python
from psycopg_pool import AsyncConnectionPool

pool = AsyncConnectionPool(dsn, min_size=5, max_size=10)
checkpoint_store = PostgresCheckpointStore.from_pool(
    pool=pool,
    consumer_name="my-consumer"
)
```

`from_pool()` uses connections from your application pool for checkpoint operations, saving the overhead of creating separate connections.

## Recovery on restart

When walbox starts:

1. It loads the durable checkpoint from the checkpoint store
2. Resumes replication from the next LSN after that checkpoint
3. If no checkpoint exists, starts from LSN 0

The checkpoint is **never** updated until `checkpoint.save()` completes successfully in your handler. If the handler crashes or raises an exception, the checkpoint doesn't advance, and walbox will redeliver the transaction on restart.

## Crash scenarios

### Before handler runs

- **Checkpoint state**: Previous durable checkpoint intact
- **On restart**: Transaction fully redelivered
- **Result**: Handler runs again on the same data

### During handler execution

- **Checkpoint state**: Previous durable checkpoint intact (handler may have partially completed)
- **On restart**: Transaction fully redelivered
- **Result**: Handler runs again; your sink must deduplicate (or use same-transaction pattern)

### After handler succeeds, before checkpoint saved

- **Checkpoint state**: Previous durable checkpoint intact
- **On restart**: Transaction fully redelivered
- **Result**: Canonical at-least-once duplicate. Durability is never sacrificed to avoid it

### During checkpoint save

- **Checkpoint state**: Previous durable checkpoint intact (current write fails/rolls back)
- **On restart**: Transaction fully redelivered
- **Result**: All work since last checkpoint is replayed

### After checkpoint durable, before feedback sent

- **Checkpoint state**: New checkpoint is durable
- **On restart**: Resumes from new checkpoint
- **Result**: The transaction does not need to be redelivered (the checkpoint is durable and is the source of truth for recovery). Replication feedback is a hint to PostgreSQL about slot management, not the application's recovery boundary.

## Checkpoint vs. replication feedback

walbox maintains two separate pieces of state:

**Your durable checkpoint**: The LSN up to which your application has successfully processed transactions and persisted proof of that processing. This is your application's source of truth for recovery.

**Replication feedback**: What walbox tells PostgreSQL about its progress via the replication protocol. Feedback is used by PostgreSQL to manage WAL retention and the replication slot.

**Important**: These can lag behind each other. If walbox crashes between saving the checkpoint and sending replication feedback to PostgreSQL, your checkpoint is durable but PostgreSQL doesn't know about it yet. On restart, walbox resumes from the durable checkpoint—no redelivery occurs. PostgreSQL might also resend transactions if its own feedback position lagged.

Conversely, if feedback advances faster than your checkpoint (which walbox prevents in normal operation), PostgreSQL could hypothetically delete WAL that contains transactions your application hasn't processed yet.

**Bottom line**: Always rely on your durable checkpoint for recovery, not on PostgreSQL's replication feedback position. Your checkpoint is the only source of truth for what your application has durably processed.

## Memory and connection considerations

- **FileCheckpointStore**: No database connection needed for checkpoint operations
- **PostgresCheckpointStore** (without `from_pool()`): Opens a separate connection to save the checkpoint
- **PostgresCheckpointStore** (with `from_pool()`): Reuses connections from your application pool, reducing overhead
- **Replication connection**: Separate, managed by walbox internally, and never shared with checkpoint or application connections

Pool size should be based on your application's concurrent database workload. Checkpointing itself requires only one connection when it runs, but your application may have other concurrent queries.

## Summary

- Checkpoints are **always manual** (your handler calls `checkpoint.save()` after confirming success)
- Checkpoints are **only updated** when `save()` completes successfully
- **Checkpoint order matters**: save the checkpoint only after your side effect is durable
- **FileCheckpointStore** for external sinks; your sink must handle redelivery via idempotency or deduplication
- **PostgresCheckpointStore** with same-transaction atomicity for exactly-once effects on PostgreSQL
- On crash, transactions are redelivered from the last durable checkpoint
- Transactions covered by a durable checkpoint do not normally need to be redelivered on restart
