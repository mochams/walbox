# Shutdown & Lifecycle

## Handler failure behavior

If your handler raises an exception, walbox does **not** catch it. The exception propagates out of `client.run()`, ending the process.

This is intentional and correct:

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

Why this design? It protects the at-least-once guarantee. If walbox silently caught handler exceptions and moved on, you could lose transactions (process continues, checkpoint advances, but the handler never actually completed). By failing hard, walbox forces you to handle errors explicitly and ensures duplicates are always possible but loss is never possible.

Implement error handling in your application if needed:

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue

        try:
            await publish_to_broker(change.new)
        except BrokerDown:
            # Handle recoverable errors
            logger.warning("broker temporarily down, will retry on restart")
            # Don't checkpoint; process will restart and retry
            raise
        except ValueError:
            # Handle non-recoverable errors
            logger.error("invalid message, skipping")
            # Checkpoint anyway to avoid infinite redelivery

    await checkpoint.save(tx.commit_lsn)
```

If you want automatic retries or backoff, implement that in your application layer, not in walbox.

## Graceful shutdown

Set up signal handlers to initiate graceful shutdown:

```python
import asyncio
import signal

async def main() -> None:
    client = ReplicationClient(options)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, client.close)

    await client.run(handle)
```

When a signal arrives (SIGTERM, SIGINT):

1. `client.close()` is called
2. The queue is shut down
3. The receiver stops accepting new messages (but keeps sending keepalives)
4. The consumer finishes the current handler (if running)
5. Both loops exit cleanly
6. A final replication status update is sent
7. The connection is closed
8. `client.run()` returns

```
Signal received
    ↓
close() called
    ↓
queue shutdown(immediate=True)
    ↓
receiver and consumer notice queue shutdown
    ↓
receiver: exits cleanly (returns from loop)
consumer: finishes current handler, exits cleanly
    ↓
TaskGroup completes normally
    ↓
final status update sent
    ↓
process exits
```

### Shutdown time

Shutdown time depends on how long your handler takes to finish. If it takes 30 seconds, shutdown takes ~30 seconds.

For Docker/Kubernetes, set a reasonable `terminationGracePeriodSeconds`:

```yaml
spec:
  terminationGracePeriodSeconds: 30
```

If your handler is hung, the process may not exit within the grace period, and the container will be forcibly killed (no different from a crash—redelivery on restart).

## Connection lifecycle

### Replication connection

- Opened on `await client.run()`
- Reconnects automatically on network failure
- Closed on graceful shutdown (final status update sent, then connection closed)
- Used only for consuming the logical replication stream

You don't manage this directly; walbox handles it.

### Checkpoint connection(s)

If using `PostgresCheckpointStore`:

- **Without a pool**: A new connection is opened for each `checkpoint.save()`, used to update the checkpoint row, then closed
- **With a pool via `from_pool()`**: Connections are taken from your pool, used for checkpoint operations, then returned to the pool

Checkpoint operations are synchronous and fast (a single INSERT or UPDATE). They add minimal latency.

### Handler connections

Your handler may need its own connections to your sink (for application writes, publishing to brokers, etc.). You manage these:

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    async with pool.connection() as conn:
        # Your application work here
        await conn.execute("INSERT INTO events (...)")

    # Don't keep connections open after handler completes
    await checkpoint.save(tx.commit_lsn)
```

walbox does **not** manage handler connections; they're your responsibility.

### Total connection count

A typical deployment uses:

- 1 replication connection (managed by walbox)
- 1 checkpoint connection (or N from a pool via `from_pool()`)
- Your handler's own connections (typically managed by a pool)

Pool size should be based on your application's concurrent database workload. See [Checkpointing & Recovery](checkpointing-recovery.md) for pool sizing guidance.

### PostgreSQL connection limits

PostgreSQL's `max_connections` parameter (default 100) limits the total. Example allocation for 5 walbox consumers:

```
5 consumers * (1 replication + 5 pooled) = 30 connections
+ application servers' connections = 20
+ superuser headroom = 5
+ buffer = 45
= 100 total
```

Leave headroom—don't fill max_connections to the brim.

## Clean vs. abrupt shutdown

| Scenario | Checkpoint saved? | Redelivery on restart? |
|---|---|---|
| Graceful shutdown (SIGTERM) | Yes (final handler completes and checkpoints) | No |
| Crash mid-handler | No | Yes (same transaction redelivered) |
| Crash before checkpoint saved | No | Yes (any unchecked transactions redelivered) |

Graceful shutdown is strictly better than a crash: it ensures the final checkpoint is saved before exiting.

## Deployment patterns

**systemd**:

```ini
[Service]
ExecStart=/usr/bin/python3 /opt/walbox/handler.py
Restart=always
RestartSec=10
KillMode=mixed
KillSignal=SIGTERM
TimeoutStopSec=30
```

**Docker**:

```dockerfile
CMD ["exec", "python", "handler.py"]
```

Use `exec` so Python is PID 1 and receives signals directly.

**Kubernetes**:

```yaml
spec:
  terminationGracePeriodSeconds: 30
```

Give walbox time to finish the handler and checkpoint before the forcible kill.

## Summary

- Handler exceptions end the process; the supervisor restarts it
- Graceful shutdown via `client.close()` finishes the handler and checkpoints before exiting
- Replication, checkpoint, and handler connections are managed separately
- Transactions covered by the final checkpoint do not normally need to be redelivered on restart
- Abrupt termination (kill, power loss) causes redelivery of unchecked work
