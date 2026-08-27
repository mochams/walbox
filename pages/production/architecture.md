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

walbox uses PostgreSQL's logical replication feature to consume committed transactions directly from the write-ahead log (WAL). This gives you several advantages:

- **No polling**: changes appear immediately, not on a polling interval
- **No log table truncation** required (PostgreSQL manages WAL retention)
- **Transactional consistency**: you see exactly what committed, not partially-written data
- **At least-once delivery**: if anything goes wrong, walbox will replay from where it left off

The key insight: because walbox reads from the WAL, it sees committed transactions before any application-layer replication tools or CDC systems would. This makes PostgreSQL the single source of truth for what happened and when.

## Transaction boundaries

walbox works at **transaction boundaries**, not at row level. This matters for two reasons:

**1. Atomicity**: Your application writes business data and an outbox/event record in the same transaction. They either both commit or both roll back. walbox preserves this boundary—you never see a transaction with only some of its rows.

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
    └─ Saves checkpoint
```

The **receiver** reads from PostgreSQL and assembles complete transactions. It keeps working even if your handler is slow, because they run independently.

The **consumer** runs your application handler one transaction at a time, in order.

Between them sits a **bounded queue** that acts as a buffer. Its size is configurable (default 10 transactions). This queue is the mechanism for backpressure.

## Backpressure

If your handler is slow, the bounded queue fills up. Once full, the receiver blocks, which stops reading from PostgreSQL. PostgreSQL's socket buffer fills, and PostgreSQL itself slows down. This is **backpressure**: it propagates from your slow handler back to PostgreSQL.

The benefit: memory usage is bounded, and PostgreSQL won't be starved of status updates or keepalives. The receiver continues sending keepalives even while backpressured on the queue.

```
Fast PostgreSQL → [full queue] ← Slow handler
                      ↑
              Receiver blocked here
```

## Checkpointing

After your handler completes successfully, walbox saves a **checkpoint**: the durable position in the replication stream up to which all transactions have been processed.

On restart, walbox resumes from the last saved checkpoint. Any transactions since that checkpoint are redelivered (because they were never checkpointed).

This is why checkpointing must happen **after** your handler succeeds, not before. If you checkpoint before your handler runs, a crash loses the transaction. If you checkpoint after, a crash replays it.

For more on checkpointing strategies, see [Checkpointing & Recovery](checkpointing-recovery.md).

## What happens when things go wrong?

If your handler crashes, the transaction is not checkpointed. When walbox restarts, it resumes from the last saved checkpoint, and the transaction is redelivered.

If PostgreSQL crashes, walbox reconnects and resumes from the last saved checkpoint. Any transactions since that checkpoint are redelivered (because PostgreSQL's replication slot tracks what we've acknowledged, not what we've checkpointed locally).

The principle: **walbox never advances the checkpoint without durably persisting it**. This means crashes always result in redelivery, never loss.

For detailed crash scenarios and their outcomes, see [Delivery Guarantees](delivery-guarantees.md) and [Checkpointing & Recovery](checkpointing-recovery.md).

## What walbox is not

walbox is a specialized tool for one pattern: the transactional outbox and event sourcing with PostgreSQL.

It is **not**:

- **A general database replication system**: it doesn't copy schema, handle DDL, replicate full tables, or track column changes. It reads the outbox table you define and delivers its rows.
- **A CDC/ETL platform**: it's not designed for building data pipelines or extracting historical data.
- **A message broker**: it doesn't offer topics, subscriptions, consumer groups, or a persistent message store. It delivers transactions to one application handler, in order.
- **A horizontal scaling tool**: one walbox consumer is single-threaded. For scale, run multiple consumers with sharded data (each handling a subset of rows).

walbox's scope is narrow: consume a PostgreSQL replication stream, assemble transactions, buffer them with backpressure, and deliver them to application code. That's it.

## Why this matters

The transactional outbox pattern solves a hard problem: reliably publishing events or updates to external systems without losing them or duplicating them excessively.

The traditional approach (polling an outbox table) is slow and doesn't scale. The walbox approach (logical replication) is fast, real-time, and leverages PostgreSQL's own durability guarantees.

By working at transaction boundaries, walbox ensures that the outbox row and your business data are always in sync. By checkpointing durably, it ensures no silent loss. By providing backpressure, it ensures your application can handle the load without memory blowup.

## Next steps

- **New to walbox?** Start with [Quickstart](../getting-started/quickstart.md)
- **How do I guarantee exactly-once effects?** See [Delivery Guarantees](delivery-guarantees.md)
- **How do I set up PostgreSQL?** See [PostgreSQL Setup](setup.md)
- **How does checkpointing work?** See [Checkpointing & Recovery](checkpointing-recovery.md)
