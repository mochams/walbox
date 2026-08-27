# Deployment

## Resource sizing

A walbox consumer is primarily **I/O-bound**. Resource usage depends on your workload and handler implementation.

### Factors that determine resource usage

**Memory**:
- `max_pending_transactions` queue size × typical transaction size
- Large streamed transactions can consume substantial memory during assembly (independent of queue size)
- Handler buffering (if your handler buffers transactions before sending)
- Python runtime overhead

**CPU**:
- Single-threaded asyncio event loop; low CPU in typical workloads
- Handler complexity (if your handler is compute-heavy, CPU will be higher)
- Broker/external I/O (non-blocking, so doesn't require more CPU)

**Network**:
- One long-lived PostgreSQL replication connection
- Checkpoint connections (or pool reuse via `from_pool()`)
- Handler's own connections to sinks/brokers (your responsibility to manage)
- Latency matters more than bandwidth for replication

**Disk**:
- Only if using `FileCheckpointStore` (small writes, ~1 KB per checkpoint)
- Checkpoint frequency affects write rate

### Capacity planning

Start by measuring your actual workload:

1. Run a consumer against production or production-like data
2. Monitor `queue_depth`, `last_handler_latency_seconds`, `replication_lag_bytes`, and process memory
3. Identify the bottleneck (handler latency? broker latency? memory?)
4. Adjust accordingly:
   - Slow handler → optimize business logic, increase resources, or add more consumers
   - Memory pressure → reduce `max_pending_transactions`, reduce handler buffering, or run on larger instance
   - High lag → check if PostgreSQL is backpressuring, check network latency to broker

Don't guess resource limits. Observe your workload and set limits with headroom.

## Running walbox

walbox is a Python application. It requires:
- Python 3.13+
- Psycopg 3 (included when you `pip install walbox`)
- Any dependencies your handler needs (broker libraries, etc.)

Configure your process manager (systemd, Docker, Kubernetes, etc.) to:
- Restart on failure
- Send SIGTERM for graceful shutdown
- Allow 30+ seconds for handler to complete before forcible kill (see [Shutdown & Lifecycle](shutdown-lifecycle.md))
- Wire metrics via `on_metrics` callback (see [Monitoring](monitoring.md))

## Multi-consumer deployments

To scale horizontally, run multiple walbox consumers. Each needs:

- Its own replication slot (distinct `slot_name`)
- Its own checkpoint store (distinct `consumer_name` or separate checkpoint store instance)
- A disjoint data subset (e.g., rows where `entity_id % num_consumers == consumer_id`)

Example:

```python
# Consumer A: processes even entity_ids
checkpoint_a = PostgresCheckpointStore(dsn, consumer_name="consumer-a")
client_a = ReplicationClient(ReplicationOptions(
    consumer_name="consumer-a",
    slot_name="slot-a",
    checkpoint_store=checkpoint_a,
    ...
))

# Consumer B: processes odd entity_ids
checkpoint_b = PostgresCheckpointStore(dsn, consumer_name="consumer-b")
client_b = ReplicationClient(ReplicationOptions(
    consumer_name="consumer-b",
    slot_name="slot-b",
    checkpoint_store=checkpoint_b,
    ...
))
```

Each consumer:
- Reconnects independently
- Maintains independent checkpoints
- Does not interfere with others

Within each consumer, transactions are processed in order. Across consumers, there is no guaranteed order (which is usually acceptable for event processing).

### Sharded handlers

For finer-grained concurrency within a single consumer, use application-level sharding. See [`examples/outbox_concurrency.py`](https://github.com/mochams/walbox/blob/main/examples/outbox_concurrency.py) for a pattern that shards rows within one consumer using bounded queues per shard.

## Monitoring

Wire metrics to your observability system via the `on_metrics` callback. Monitor:

- **replication_lag_bytes**: Growing lag indicates walbox is falling behind
- **queue_depth**: High queue means handler is slower than inbound rate
- **last_handler_latency_seconds**: Latency spikes indicate slow handler
- **reconnect_count**: Increasing count suggests network or PostgreSQL issues
- **checkpoint_lsn**: Should advance with `transactions_processed`; if stuck, handler may not be checkpointing

See [Monitoring](monitoring.md) for alert strategies and examples.

## Summary

- Resource usage is workload-dependent; measure and adjust
- I/O-bound: network latency matters, CPU is low
- Memory scales with transaction size and queue depth
- Run multiple consumers for horizontal scale (each with separate slot/checkpoint)
- Wire metrics to observability system for visibility
- Allow 30+ seconds for graceful shutdown
