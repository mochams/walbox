# Architecture

## From PostgreSQL transaction to application event

walbox transforms committed PostgreSQL transactions into reliable application events. The flow is straightforward:

```
PostgreSQL transaction commits
    ↓
Transaction appears in logical replication stream
    ↓
walbox receives and decodes the stream
    ↓
walbox assembles complete transactions
    ↓
Application handler processes the transaction
    ↓
Checkpoint is saved durably
    ↓
Replication progress advances
```

This is the essential guarantee: a committed transaction either reaches your application and is checkpointed, or it doesn't. There is no in-between state.

## PostgreSQL is the source of truth

walbox uses PostgreSQL's logical replication feature to consume committed transactions directly from the write-ahead log (WAL). Every position in the WAL has an LSN, PostgreSQL's log sequence number, and that's what a checkpoint records. This gives you several advantages:

- **No polling**: changes appear immediately, not on a polling interval
- **No log table truncation** required (PostgreSQL manages WAL retention)
- **Transactional consistency**: you see exactly what committed, not partially-written data
- **At least-once delivery**: if anything goes wrong, walbox will replay from where it left off

The key insight: because walbox reads from the WAL, it sees committed transactions before any application-layer replication tools or CDC systems would. This makes PostgreSQL the single source of truth for what happened and when.

This is the same problem trigger-based CDC and `LISTEN`/`NOTIFY` solve less durably: logical replication gives you a crash-safe, ordered stream without extra trigger tables or a fragile in-process pub/sub channel that drops messages if nobody's listening.

## Transaction boundaries

walbox works at **transaction boundaries**, not at row level. This matters for two reasons:

**1. Atomicity**: Your application writes business data and an outbox/event record in the same transaction. They either both commit or both roll back. walbox preserves this boundary. You never see a transaction with only some of its rows.

```
Your application:
    BEGIN
        UPDATE accounts SET balance = ...
        INSERT INTO outbox (event) VALUES (...)
    COMMIT

walbox sees and delivers:
    [complete transaction with both the account change and the event]
```

**2. Ordering**: Transactions preserve order. walbox delivers them in commit order, so if transaction A committed before transaction B, your handler will see A before B. This is essential for event sourcing and maintaining consistency in projection tables.

## Inside the walbox pipeline

walbox splits the work into two parallel stages: **receiving** and **consuming**.

```
PostgreSQL replication stream
    ↓
Replication receiver
    ├─ Decodes replication protocol
    ├─ Assembles transactions (including large streamed transactions)
    └─ Enqueues complete transactions
    ↓
Bounded delivery queue (backpressure)
    ↓
Application consumer
    ├─ Runs your handler
    │      ↓
    │  your broker, database, or HTTP endpoint
    └─ Saves checkpoint
```

The **receiver** reads from PostgreSQL and assembles complete transactions. It keeps working even if your handler is slow, because they run independently.

The **consumer** runs your application handler one transaction at a time, in order.

Between them sits a **bounded queue** that acts as a buffer. Its size is configurable (default 10 transactions). This queue is the mechanism for backpressure.

## Connections

A walbox consumer opens up to three kinds of connections, each managed differently:

**Replication connection**: opened when you call `client.run()`, reconnects automatically on network failure, and closes on graceful shutdown after a final status update. walbox manages this end to end; you never touch it directly.

**Checkpoint connection**: built by `WalboxBuilder`. With `build()`, walbox opens a new connection for each `checkpoint.save()` call and closes it right after. With `build_with_pool()`, it borrows a connection from your own pool instead. Either way, a checkpoint save is a single INSERT or UPDATE, so it's fast and adds little latency at any volume — but the per-call connect/disconnect with `build()` still adds up under frequent checkpointing. Prefer `build_with_pool()` unless you have a reason not to add the `psycopg-pool` dependency.

**Your handler's connections**: anything your handler opens to reach its own sink, a broker client, a database pool, an HTTP client, is your responsibility to manage. walbox doesn't know about these and won't close them for you.

## Backpressure

If your handler is slow, the bounded queue fills up. Once full, the receiver blocks, which stops reading from PostgreSQL. PostgreSQL's socket buffer fills, and PostgreSQL itself slows down. This is **backpressure**: it propagates from your slow handler back to PostgreSQL.

The benefit: memory usage is bounded, and PostgreSQL won't be starved of status updates or keepalives. The receiver continues sending keepalives even while backpressured on the queue.

```
Fast PostgreSQL → [full queue] ← Slow handler
                      ↑
              Receiver blocked here
```

See [Monitoring & Backpressure](monitoring.md) for tuning the queue size and watching for it in your metrics.

## Checkpointing

After your handler completes successfully, walbox saves a **checkpoint**: the durable position in the replication stream up to which all transactions have been processed.

walbox resumes from the last durable checkpoint after restart, ensuring committed transactions are not silently skipped. Any transactions since that checkpoint are redelivered (because they were never checkpointed).

This is why checkpointing must happen **after** your handler succeeds, not before. If you checkpoint before your handler runs, a crash loses the transaction. If you checkpoint after, a crash replays it.

For more on checkpointing strategies, see [Delivery Guarantees](delivery-guarantees.md).

## What happens when things go wrong?

If your handler crashes, the transaction is not checkpointed. When walbox restarts, it resumes from the last saved checkpoint, and the transaction is redelivered.

If PostgreSQL crashes, walbox reconnects and resumes from the last saved checkpoint. Any transactions since that checkpoint are redelivered (because PostgreSQL's replication slot tracks what we've acknowledged, not what we've checkpointed locally).

The principle: **walbox never advances the checkpoint without durably persisting it**. This means crashes always result in redelivery, never loss.

For detailed crash scenarios and their outcomes, see [Delivery Guarantees](delivery-guarantees.md).

## Graceful shutdown

Wire `client.close()` to your process's termination signals:

```python
import asyncio
import signal

async def main() -> None:
    client = WalboxBuilder.build(options)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, client.close)

    await client.run(handle)
```

When a signal arrives, walbox stops pulling new transactions off the replication stream (but keeps sending keepalives), lets the handler call already in progress finish, sends a final status update, and closes the connection. `client.run()` then returns.

This is not a full drain. Anything already decoded and sitting in the delivery queue when `close()` is called is dropped, not processed. That's fine: none of it was checkpointed yet, so it's simply redelivered the next time walbox runs. Shutdown time is bounded by how long your handler takes to finish, so give your process manager enough grace period to match. See [Setup & Deployment](setup.md) for sizing that grace period.

## Next steps

- **New to walbox?** Start with [Quickstart](../getting-started/quickstart.md)
- **How do I guarantee exactly-once effects?** See [Delivery Guarantees](delivery-guarantees.md)
- **How do I set up PostgreSQL?** See [Setup & Deployment](setup.md)
