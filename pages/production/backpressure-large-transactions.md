# Backpressure & Large Transactions

!!! warning "Important limitation"
    `max_pending_transactions` bounds the completed transactions waiting for your handler, but **does not cap the memory consumed by large streamed transactions**. A single PostgreSQL transaction can consume substantially more memory than the queue limit suggests. See [Large streamed transactions](#large-streamed-transactions) below.

## At a glance

walbox uses a **bounded delivery queue** between the receiver (reading from PostgreSQL) and the consumer (running your handler):

```
PostgreSQL
    ↓
Replication receiver
    ↓
Transaction assembly (unbounded for large streamed transactions)
    ↓
Bounded delivery queue (← backpressure applies here)
    ↓
Handler
```

The **delivery queue** is bounded (default 10 transactions). The **transaction assembly** buffer is not.

## What the queue protects you from

The bounded queue prevents a slow handler from starving Postgres keepalives and limits the number of fully-assembled transactions waiting to be processed.

When the queue is full:

1. The receiver blocks trying to enqueue the next transaction
2. The receiver stops reading from PostgreSQL
3. PostgreSQL's socket buffer fills up
4. PostgreSQL slows down (backpressure propagates upstream)
5. Your handler processes transactions and drains the queue
6. The receiver unblocks and resumes reading

**Key**: The receiver continues sending status updates to PostgreSQL even while blocked on a full queue (every `status_interval` seconds, default 10). This prevents keepalive timeouts and ensures PostgreSQL doesn't think the connection is dead.

## What the queue does not protect you from

The delivery queue bounds *completed transactions waiting for the handler*, but not:

- **Streamed transaction assembly**: PostgreSQL can send very large transactions in chunks (StreamStart → data chunks → StreamStop). walbox accumulates these chunks in memory while assembling the complete transaction, independent of the queue size. A 1 GB transaction will consume ~1 GB of memory while being assembled, even if the queue is empty.

- **Handler latency**: If your handler is slow, the queue fills. This is visible in metrics but isn't prevented by the queue itself—it's the mechanism walbox uses to apply backpressure.

## Queue size configuration

```python
options = ReplicationOptions(
    ...
    max_pending_transactions=10,  # Default
)
```

Queue size is a tradeoff:

- **Smaller (2-5)**: Less memory for buffered transactions; requires the handler to be consistently fast. Use when memory is constrained.
- **Larger (50+)**: More buffering for burst workloads; tolerates temporary handler slowness. Use when throughput is bursty or unpredictable.

The default of 10 is conservative and suitable for most workloads.

**Rough memory estimate** (for planning, not precise calculation):

- Queue size × average transaction size = approximate queue memory
- Example: 10 transactions × 10 KB average = ~100 KB queue memory
- This is a conceptual estimate; actual memory overhead includes Python object headers and internal buffers

## Large streamed transactions

PostgreSQL sends very large transactions in chunks to avoid buffering them on the server. walbox must reassemble these chunks into a complete transaction before your handler can process it.

### The lifecycle

```
PostgreSQL StreamStart message
    ↓
walbox accumulates chunks in memory (TransactionAssembler)
    ↓
more StreamStart/intermediate chunks arrive
    ↓
PostgreSQL StreamCommit message
    ↓
complete Transaction enters delivery queue
    ↓
handler receives the fully assembled Transaction
    ↓
checkpoint saved
```

Your handler receives the **complete, fully assembled transaction**, not streamed data. This is a fundamental design choice: walbox preserves transaction boundaries for correctness.

### Memory implications

Memory is consumed during assembly (steps 1-4 above), before the transaction enters the queue. This means:

- A large transaction can spike memory even if the queue is empty
- Queue depth does not reflect transaction-assembly memory usage
- There is currently **no configurable limit on transaction-assembly memory**

### What you can do

If large transactions are a concern:

1. **Keep individual PostgreSQL transactions reasonably sized** (when practical). This is the most effective mitigation—smaller transactions mean smaller assembly buffers.

2. **Avoid unnecessarily large payloads in outbox rows**. PostgreSQL's TOAST compression helps, but smaller rows are better for walbox's memory usage.

3. **Process rows one at a time in your handler**, without buffering the entire transaction before sending. This reduces application-level memory, but does not reduce walbox's transaction-assembly memory:

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    # Good: process each row individually
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue

        # Send immediately, don't buffer in application
        await broker.publish(change.new)

    # Checkpoint only after all rows are sent
    await checkpoint.save(tx.commit_lsn)
```

1. **Monitor process memory** for workloads that can produce large transactions. Use OS-level tools or container memory limits.

2. **Allocate sufficient memory** in your deployment. Capacity planning should account for large transactions as a realistic scenario, not an edge case.

3. **Evaluate whether walbox is appropriate** for your workload if you regularly handle multi-gigabyte transactions. Extreme cases may require different architectures.

## Monitoring backpressure

walbox emits metrics every `status_interval` seconds (default 10):

```python
async def on_metrics(metrics: Metrics) -> None:
    queue_depth = metrics.queue_depth  # Note: queue_depth, not pending_transactions
    if queue_depth > options.max_pending_transactions * 0.8:
        logger.warning(f"backpressure engaged: {queue_depth}/{options.max_pending_transactions}")

options = ReplicationOptions(
    ...
    on_metrics=on_metrics,
)
```

Watch for:

- **Consistently high queue depth**: If `queue_depth` regularly approaches `max_pending_transactions`, backpressure is active and your handler can't keep up
- **Queue at maximum**: If the queue repeatedly hits its limit, optimization is needed
- **Handler latency spikes**: Check `metrics.last_handler_latency_seconds` for slow handler calls
- **Process memory growth**: Monitor system memory, especially during large transactions

## Horizontal scaling with sharding

walbox provides single-consumer semantics per replication slot, which is essential for durability and ordering guarantees. To scale horizontally, run multiple walbox consumers on disjoint subsets of your data.

Each consumer:

- Reads a disjoint subset of rows (e.g., based on `entity_id % number_of_shards`)
- Has its own replication slot
- Has its own checkpoint store
- Maintains ordering within its shard

Within each shard, transactions are processed in order. Across shards, order is not guaranteed, which is usually acceptable for event processing.

### PostgreSQL operational considerations

Each replication slot maintains its own position in the WAL. If one consumer is slow or crashes, its slot may cause PostgreSQL to retain WAL longer than others. Monitor replication lag per slot and investigate slow consumers.

### Example pattern

The pattern from [`examples/outbox_concurrency.py`](https://github.com/mochams/walbox/blob/main/examples/outbox_concurrency.py) demonstrates:

1. Assign each outbox row to a shard based on `entity_id % N`
2. Create one walbox consumer per shard
3. Each consumer has its own queue and processes rows for that shard
4. Multiple consumers run in parallel, processing different shards

This provides horizontal scaling while preserving single-consumer crash recovery semantics.

## Summary

- The delivery queue is **bounded** (prevents unlimited buffering of completed transactions)
- Transaction assembly is **not bounded** (large streamed transactions can use substantial memory)
- Backpressure is automatic: a full queue slows the receiver, which slows PostgreSQL
- Monitor `queue_depth` in metrics to detect handler slowness
- Large transactions should be part of your capacity planning, not treated as edge cases
- For horizontal scale, run multiple consumers with sharded subscriptions (each with its own slot and checkpoint)
