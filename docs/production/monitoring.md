# Monitoring

walbox emits periodic metrics you can hook into for observability and alerting. This guide covers what metrics are available, how to wire them to your monitoring system, and what signals indicate problems.

## Metrics at a glance

Register a callback via `on_metrics` in `WalboxOptions`:

```python
from walbox import Metrics, WalboxOptions

def on_metrics(metrics: Metrics) -> None:
    # Invoked periodically (every status_interval, default 10 seconds)
    print(f"Processed: {metrics.transactions_processed}")
    print(f"Queue: {metrics.queue_depth}")
    print(f"Checkpoint LSN: {metrics.checkpoint_lsn}")

options = WalboxOptions(
    consumer_name="my-consumer",
    dsn="postgresql://...",
    slot_name="outbox_slot",
    publication_name="walbox_pub",
    on_metrics=on_metrics,
)
```

**Important**: `on_metrics` is **synchronous** (not async). If you need to send metrics to an external system, spawn a non-blocking task instead of blocking in the callback.

## Available metrics

| Field | Type | Description |
| --- | --- | --- |
| `consumer_name` | str | The `consumer_name` from `WalboxOptions`, identifying which consumer this snapshot is from |
| `receive_lsn` | int | Highest WAL LSN received from PostgreSQL |
| `checkpoint_lsn` | int | Durable application checkpoint position |
| `replication_lag_bytes` | int | Bytes between receive_lsn and last keepalive |
| `transactions_processed` | int | Total transactions dispatched to handler (cumulative) |
| `changes_processed` | int | Total row changes seen (cumulative) |
| `reconnect_count` | int | Number of reconnections since process start |
| `last_handler_latency_seconds` | float | Duration of the last handler call |
| `queue_depth` | int | Current delivery queue size |
| `last_keepalive_at` | float | Timestamp of last PostgreSQL keepalive |
| `last_checkpoint_latency_seconds` | float | Duration of the last checkpoint save |
| `transactions_since_checkpoint` | int | Transactions queued since last checkpoint |

## Understanding the metrics

### Replication progress

- **`receive_lsn`**: The highest WAL position walbox has received. Moves forward as PostgreSQL sends data.
- **`checkpoint_lsn`**: The durable checkpoint position. Represents what your application has successfully processed and persisted. **This is your source of truth for recovery.**
- **`replication_lag_bytes`**: Approximate bytes between what walbox has received and what PostgreSQL has confirmed. High lag indicates walbox can't consume fast enough.

### Processing throughput

- **`transactions_processed`**: Total transactions sent to your handler since this process started. Incremented before the handler runs, so it counts dispatched transactions, not necessarily successfully checkpointed ones.
- **`changes_processed`**: Total row changes (inserts, updates, deletes) seen. Useful for throughput calculation.
- **`transactions_since_checkpoint`**: How many transactions have been processed since the last checkpoint save. Resets to 0 after each checkpoint.

### Handler performance

- **`last_handler_latency_seconds`**: How long the last handler call took. Useful for detecting slow handlers.
- **`queue_depth`**: Current size of the delivery queue (0 to `max_pending_transactions`). High depth means your handler is slower than incoming rate.
- **`last_checkpoint_latency_seconds`**: How long the last checkpoint save took. Spikes indicate slow checkpoint store (network, disk I/O).

### Connection health

- **`reconnect_count`**: How many times walbox has reconnected. Increasing count indicates network issues or PostgreSQL restarts.
- **`last_keepalive_at`**: Timestamp (from `asyncio.get_event_loop().time()`) of the last keepalive received from PostgreSQL. Use this to detect stalled connections.

## Backpressure

walbox uses a bounded delivery queue between the receiver, which reads from PostgreSQL, and the consumer, which runs your handler (see [Architecture](architecture.md#inside-the-walbox-pipeline)). The queue itself is bounded (`max_pending_transactions`, default 10). Assembling an individual transaction is not.

When the queue is full:

1. The receiver blocks trying to enqueue the next transaction
2. The receiver stops reading from PostgreSQL
3. PostgreSQL's socket buffer fills up
4. PostgreSQL slows down; backpressure has propagated upstream
5. Your handler processes transactions and drains the queue
6. The receiver unblocks and resumes reading

The receiver keeps sending status updates to PostgreSQL throughout, even while blocked on a full queue (every `status_interval` seconds, default 10). This keeps PostgreSQL from thinking the connection is dead.

The queue bounds *completed transactions waiting for the handler*. It does not bound the memory used to assemble a single large transaction; see [Large transactions](#large-transactions) below.

### Queue size

```python
options = WalboxOptions(
    ...
    max_pending_transactions=10,  # Default
)
```

Queue size is a tradeoff:

- **Smaller (2-5)**: less memory for buffered transactions, but requires the handler to stay consistently fast. Use when memory is constrained.
- **Larger (50+)**: more buffering for burst workloads, tolerating temporary handler slowness. Use when throughput is bursty or unpredictable.

The default of 10 is conservative and suits most workloads. As a rough planning estimate, queue size times average transaction size gives you the approximate queue memory: 10 transactions at 10 KB average is about 100 KB, though actual overhead adds Python object headers and internal buffers on top of that.

## Large transactions

PostgreSQL can send a very large transaction in chunks rather than all at once. walbox buffers those chunks in memory as they arrive and only hands your handler the complete, assembled transaction once it commits, never partial data. This buffering happens before the transaction reaches the delivery queue, so a single very large transaction can use memory proportional to its own size, regardless of how small your queue is. A 1 GB transaction uses roughly 1 GB of memory while being assembled, even with an empty queue.

There is currently no configurable limit on this assembly memory. If large transactions are a concern:

1. **Keep individual PostgreSQL transactions reasonably sized**, when practical. This is the most effective mitigation, since smaller transactions mean smaller assembly buffers.
2. **Avoid unnecessarily large payloads in outbox rows.** PostgreSQL's TOAST compression helps, but smaller rows are still better for walbox's memory usage.
3. **Process rows one at a time in your handler**, without buffering the entire transaction before sending:

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

   This reduces your application's own memory use, though it doesn't reduce walbox's transaction-assembly memory.
4. **Monitor process memory** for workloads that can produce large transactions, using OS-level tools or container memory limits.
5. **Plan capacity for large transactions as a realistic scenario**, not an edge case (see [Setup & Deployment](setup.md#resource-sizing)).
6. **Evaluate whether walbox fits your workload** if you regularly handle multi-gigabyte transactions. Extreme cases may need a different architecture.

## What to monitor

### Replication lag

```python
def on_metrics(metrics: Metrics) -> None:
    if metrics.replication_lag_bytes > 10_000_000:  # 10 MB lag
        logger.warning(f"High replication lag: {metrics.replication_lag_bytes} bytes")
```

Growing `replication_lag_bytes` indicates walbox can't consume as fast as PostgreSQL is producing. Check handler latency and queue depth.

### Queue depth and backpressure

```python
def on_metrics(metrics: Metrics) -> None:
    if metrics.queue_depth > max_pending_transactions * 0.8:
        logger.warning(f"Queue filling up: {metrics.queue_depth}/{max_pending_transactions}")
```

High queue depth means your handler is slower than the inbound rate. Optimize the handler or increase `max_pending_transactions` (if memory allows).

### Handler latency

```python
def on_metrics(metrics: Metrics) -> None:
    if metrics.last_handler_latency_seconds > 5:
        logger.warning(f"Slow handler: {metrics.last_handler_latency_seconds}s")
```

Handler latency spikes often indicate slow I/O, slow external brokers, or large transactions being processed.

### Checkpoint progress

```python
def on_metrics(metrics: Metrics) -> None:
    if metrics.transactions_since_checkpoint > 1000:
        logger.warning(f"Many transactions since checkpoint: {metrics.transactions_since_checkpoint}")
```

High `transactions_since_checkpoint` can indicate your handler isn't calling `checkpoint.save()` regularly, or the checkpoint store is slow.

### Connection health

```python
def on_metrics(metrics: Metrics) -> None:
    if metrics.reconnect_count > 10:
        logger.error(f"Too many reconnections: {metrics.reconnect_count}")

    now = time.time()
    if now - metrics.last_keepalive_at > 30:
        logger.warning("No keepalive from PostgreSQL in 30+ seconds")
```

Increasing `reconnect_count` suggests network issues or PostgreSQL instability. Stale `last_keepalive_at` suggests the connection is hung.

## Alerting strategy

Don't alert on single metrics in isolation. Combine signals:

- **High lag + high queue + high latency** → Handler is bottleneck
- **High lag + normal queue** → PostgreSQL is producing faster than handler can consume
- **Increasing reconnect count** → Network or PostgreSQL instability
- **Transactions processed growing, but queue empty** → System is healthy, no backpressure
- **Transactions processed growing, checkpoint stable** → Handler isn't saving checkpoints; investigate

**Important**: An idle consumer (no incoming transactions) will have zero `transactions_processed` growth, low queue depth, and normal latencies. This is not an error. Only alert if lag is growing while you expect data to flow.

## Sending metrics to external systems

The callback is synchronous. To send metrics to Prometheus, StatsD, or other systems without blocking:

```python
import asyncio
from walbox import Metrics

metric_queue: asyncio.Queue = asyncio.Queue()

def on_metrics(metrics: Metrics) -> None:
    # Non-blocking queue put; exporters run in background
    try:
        metric_queue.put_nowait(metrics)
    except asyncio.QueueFull:
        pass  # Skip this metric if queue is full

async def export_metrics():
    """Background task to send metrics to external system."""
    while True:
        metrics = await metric_queue.get()
        try:
            # Send to Prometheus, StatsD, CloudWatch, etc.
            # Example: prometheus_client
            transactions_counter.inc(metrics.changes_processed)
            queue_depth_gauge.set(metrics.queue_depth)
            lag_gauge.set(metrics.replication_lag_bytes)
        except Exception as e:
            logger.exception("failed to export metrics")

# Start export task before client.run()
asyncio.create_task(export_metrics())
```

Or use a library that handles this pattern (e.g., `prometheus-client` with a CollectorRegistry).

## Checkpointing insights

To monitor checkpoint progress independently:

Query the checkpoint table:

```sql
SELECT consumer_name, checkpoint_lsn, updated_at
FROM walbox_checkpoint
WHERE consumer_name = 'my-consumer';
```

The LSN and timestamp tell you:

- How old the checkpoint is
- How frequently checkpoints are saved
- How much lag exists between receipt and durability

## Logging

walbox uses Python's standard `logging` module. Enable logging for observability:

```python
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("walbox")
```

Key log messages appear at INFO level:

- Connection established / lost / reconnecting
- Backpressure engaged / relieved
- Shutdown initiated / complete

Use DEBUG level for verbose protocol details.

## Summary

- Metrics are emitted periodically (every `status_interval`, default 10 seconds) as a synchronous callback
- Monitor replication lag, queue depth, handler latency, and checkpoint frequency
- Combine multiple signals to avoid false alerts on idle consumers
- Inspect `checkpoint_lsn` to understand durable application progress (not `receive_lsn`)
- Use Python's `logging` module for operational visibility
