# Monitoring

walbox emits periodic metrics you can hook into for observability and alerting. This guide covers what metrics are available, how to wire them to your monitoring system, and what signals indicate problems.

## Metrics at a glance

Register a callback via `on_metrics` in `ReplicationOptions`:

```python
from walbox import Metrics, ReplicationOptions

def on_metrics(metrics: Metrics) -> None:
    # Invoked periodically (every status_interval, default 10 seconds)
    print(f"Processed: {metrics.transactions_processed}")
    print(f"Queue: {metrics.queue_depth}")
    print(f"Checkpoint LSN: {metrics.checkpoint_lsn}")

options = ReplicationOptions(
    consumer_name="my-consumer",
    dsn="postgresql://...",
    slot_name="outbox_slot",
    publication_name="walbox_pub",
    checkpoint_store=checkpoint_store,
    on_metrics=on_metrics,
)
```

**Important**: `on_metrics` is **synchronous** (not async). If you need to send metrics to an external system, spawn a non-blocking task instead of blocking in the callback.

## Available metrics

| Field | Type | Description |
| --- | --- | --- |
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

For `PostgresCheckpointStore`, query the checkpoint table:

```sql
SELECT consumer_name, checkpoint_lsn, updated_at
FROM walbox_checkpoint
WHERE consumer_name = 'my-consumer';
```

For `FileCheckpointStore`, read the checkpoint file directly.

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
