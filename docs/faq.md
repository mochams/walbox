# FAQ

## Does walbox guarantee exactly-once delivery?

No, walbox guarantees at-least-once delivery: your handler may be called with the same transaction more than once. Exactly-once *effects* are achievable in your application through an idempotent or deduplicating sink, or by saving the checkpoint in the same transaction as your write. See [Delivery Guarantees](production/delivery-guarantees.md).

## Does walbox checkpoint automatically?

No. Your handler calls `checkpoint.save(tx.commit_lsn)` itself. walbox never checkpoints on its own, since it has no way to know whether your side effect actually became durable. See [Delivery Guarantees](production/delivery-guarantees.md#manual-checkpointing).

## What happens if my handler raises an exception?

walbox doesn't catch it. The exception propagates, the transaction isn't checkpointed, and the process exits. On restart, the transaction is redelivered. This is deliberate: silently swallowing a handler failure risks the checkpoint advancing past a transaction that was never actually processed. See [Handler failure behavior](production/delivery-guarantees.md#handler-failure-behavior).

## Can multiple consumers share one replication slot?

No, each consumer needs its own replication slot. To scale horizontally, run multiple consumers, each with a distinct `slot_name` and a disjoint slice of the data (for example, sharded by `entity_id`). See [Multi-consumer deployments](production/setup.md#multi-consumer-deployments).

## Is `on_metrics` async?

No, it's a synchronous callback. Blocking in it stalls replication reads and handler dispatch, not just the metrics export. Hand the snapshot off to a queue or task instead. See [Monitoring](production/monitoring.md) and [`examples/metrics.py`](https://github.com/mochams/walbox/blob/main/examples/metrics.py).

## What happens with very large transactions?

walbox buffers a transaction in memory while assembling it, before it reaches the delivery queue, so a large transaction uses memory proportional to its own size regardless of queue settings. There's no configurable limit today. See [Large transactions](production/monitoring.md#large-transactions).

## What checkpoint backends are supported?

PostgreSQL only. This is a deliberate scope decision, not a current limitation walbox intends to lift. See [Checkpoint stores](production/delivery-guarantees.md#checkpoint-stores).

## What Python version does walbox need?

3.13 or later.

## Does walbox create my publication or table for me?

No. You create the publication and any tables it covers; walbox only creates the replication slot, and reuses it if it already exists. See [What you need to create](production/setup.md#what-you-need-to-create).

## Does walbox work with pgbouncer?

Yes, on pgbouncer 1.23.0 or later, in `session` or `transaction` pool_mode. Older pgbouncer and `statement` pool_mode don't work, for either the replication connection or the checkpoint store. See [PgBouncer](production/setup.md#pgbouncer).
