# Delivery Guarantees

walbox provides **at-least-once delivery** with a durable checkpoint. This page explains what that means, when duplicates can occur, how checkpointing works, and how to achieve exactly-once effects in your application.

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

1. **PostgreSQL commits** a transaction in your database
2. **walbox receives it** via logical replication
3. **walbox delivers it** to your handler
4. **Your handler runs** to completion
5. **Checkpoint is saved** (you call `checkpoint.save(tx.commit_lsn)`)
6. **Replication feedback is sent** to PostgreSQL

If the process crashes or exits at any point before step 5 completes, the checkpoint is not durable, and the transaction will be redelivered on restart.

If the process crashes after step 5 completes, the transaction will not be redelivered (the checkpoint is durable).

## Manual checkpointing

**Checkpointing is explicit and manual.** Your handler must call `checkpoint.save(tx.commit_lsn)` itself. walbox never checkpoints automatically, because it has no way of knowing whether your external side effect has become durable.

This is intentional: walbox isn't responsible for the durability of your sink. If you send a message to a broker or an HTTP endpoint, verify success before saving the checkpoint. The correct pattern is:

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    # 1. Send side effect
    await broker.publish(message)  # or equivalent

    # 2. Only after verifying success, save checkpoint
    await checkpoint.save(tx.commit_lsn)
```

**Incorrect pattern** (loses data on crash):

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    # WRONG: checkpoint saved before side effect is confirmed
    await checkpoint.save(tx.commit_lsn)

    # If this crashes, the checkpoint advances but the side effect may not be durable
    await broker.publish(message)
```

A crash between the checkpoint save and the side effect becoming durable loses that event: the checkpoint has already moved past it, so it will never be redelivered.

## Handler failure behavior

If your handler raises an exception, walbox does not catch it. The exception propagates out of `client.run()` and ends the process:

```
handler raises exception
    ↓
transaction is not checkpointed
    ↓
consumer task exits
    ↓
replication client stops
    ↓
process exits with error
    ↓
supervisor restarts the process
    ↓
transaction is redelivered
```

This is deliberate. It protects the at-least-once guarantee: if walbox silently caught handler exceptions and moved on, the checkpoint could advance past a transaction that was never actually handled, and that transaction would be lost, not just duplicated. Failing hard means duplicates are always possible, but loss never is.

Implement whatever error handling your application needs:

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue

        try:
            await publish_to_broker(change.new)
        except BrokerDown:
            # Recoverable: don't checkpoint, let the process restart and retry
            logger.warning("broker temporarily down, will retry on restart")
            raise
        except ValueError:
            # Non-recoverable: checkpoint anyway to avoid redelivering forever
            logger.error("invalid message, skipping")

    await checkpoint.save(tx.commit_lsn)
```

Retries and backoff are your application's job, not walbox's.

## Checkpoint stores

PostgreSQL is the only supported checkpoint backend. `WalboxBuilder` constructs a `PostgresCheckpointStore` for you; the common path never needs to name it directly.

### PostgresCheckpointStore

Stores the checkpoint as a row in your PostgreSQL database. It creates a `walbox_checkpoint` table automatically and loads the latest row for your consumer name on startup.

**Without a connection** (`checkpoint.save(lsn)`, what `WalboxBuilder.build()`/`build_with_pool()` give you): walbox opens its own connection (or one from the pool), upserts the row, and commits immediately. The checkpoint is durable once that commit completes. This is also what covers the external-sink case (a broker, an HTTP endpoint) where you can't share a transaction with the checkpoint: the checkpoint and your side effect aren't committed atomically, so if walbox crashes after the side effect succeeds but before the checkpoint is durable, the transaction is delivered again on restart. Your sink needs to tolerate that through idempotency or deduplication.

**With a caller's connection** (`checkpoint.save(lsn, connection=conn)`): walbox upserts on your connection and does not commit; you do. The checkpoint then participates in your transaction and is durable only when your transaction commits. This is what makes the same-transaction pattern below work.

## Exactly-once effects: external brokers

For external systems (a broker, a queue, a webhook endpoint), give each message a stable idempotency key, typically the outbox row's `id`:

```python
await broker.publish(key=f"outbox-{change.new['id']}", value=change.new)
```

If walbox redelivers the transaction, the receiving system sees the same key again. Whether that's enough depends on the system: some deduplicate automatically, others need you to implement it yourself. An HTTP webhook, for example, doesn't become idempotent just because you attached a header; the receiver has to check it. See the [Examples](../examples/transactional-outbox.md) for worked patterns per broker.

## Exactly-once effects: PostgreSQL sink

When your sink is PostgreSQL itself, save the checkpoint in the same transaction as your application write:

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    async with pool.connection() as conn:
        async with conn.transaction():
            for change in tx.changes:
                if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
                    continue

                await conn.execute(
                    "INSERT INTO events (...) VALUES (...)",
                    (change.new["entity_type"], ...),
                )

            # Save checkpoint in the SAME transaction
            await checkpoint.save(tx.commit_lsn, connection=conn)
            # conn.transaction() commits here
```

Either both the application write and the checkpoint update commit, or neither does. If the process crashes before the commit, both roll back, and the transaction is redelivered on restart with nothing duplicated in the database.

This requires:

- Your application state and checkpoint live in the **same PostgreSQL database**
- Both go through the **same transaction** (same connection, single commit)
- The checkpoint is written with `checkpoint.save(lsn, connection=conn)`

For a fuller version of this pattern, including a pooled connection via `WalboxBuilder.build_with_pool()`, see [the PostgreSQL example](../examples/postgresql.md).

## Recovery on restart

When walbox starts, it loads the durable checkpoint from the checkpoint store and resumes replication from the next LSN after it. If no checkpoint exists yet, it starts from LSN 0.

The checkpoint only advances when `checkpoint.save()` completes successfully inside your handler. If the handler crashes or raises before that, the checkpoint doesn't move, and the transaction is redelivered.

## Crash scenarios

| Scenario | Checkpoint state | On restart |
| --- | --- | --- |
| Before handler runs | Previous checkpoint intact | Transaction fully redelivered |
| During handler execution | Previous checkpoint intact | Transaction fully redelivered; your sink must tolerate a partially-applied, repeated attempt |
| After handler succeeds, before checkpoint saved | Previous checkpoint intact | Transaction redelivered. This is the canonical at-least-once duplicate; durability is never sacrificed to avoid it |
| During checkpoint save | Previous checkpoint intact (the write itself failed or rolled back) | Transaction redelivered |
| After checkpoint durable, before feedback sent | New checkpoint is durable | Resumes from the new checkpoint; that transaction does not need to be redelivered |
| Graceful shutdown (SIGTERM) | In-flight handler finishes and checkpoints before exit | No redelivery for that transaction; anything still queued behind it is redelivered next run |

## Checkpoint and replication feedback

walbox tracks two separate things: your durable checkpoint, and the replication feedback it sends PostgreSQL. Your checkpoint is the application's source of truth for what's been processed. Feedback is a hint PostgreSQL uses for WAL retention and slot management, and it can lag behind your checkpoint by a few seconds.

If walbox crashes between saving the checkpoint and sending feedback, PostgreSQL may resend transactions you've already checkpointed. That's safe, since your handler is already expected to be idempotent. Always recover from your own checkpoint store, never from PostgreSQL's feedback position.

## Summary

- walbox guarantees at-least-once delivery, not exactly-once
- Checkpointing is always manual: your handler calls `checkpoint.save()` only after confirming its side effect is durable
- Duplicates are expected; your handler must tolerate redelivery
- Durability comes first: replaying a transaction is preferable to losing one
- Uncaught handler exceptions end the process before checkpointing, by design
- External brokers need idempotent ingestion or consumer-side deduplication
- PostgreSQL sinks can be made exactly-once through same-transaction atomicity with `PostgresCheckpointStore`
- Your checkpoint is the source of truth for recovery, not replication feedback
