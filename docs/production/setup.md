# Setup & Deployment

walbox is a PostgreSQL logical replication client. It connects directly to PostgreSQL, reads the replication stream, and delivers transactions to your application.

Unlike a PostgreSQL SUBSCRIPTION, walbox does not:

- Create a subscriber database
- Perform initial table synchronization (COPY)
- Store the replication slot in PostgreSQL's own catalogs

walbox manages the replication slot and checkpoint independently.

## What you need to configure

### `wal_level`

Set `wal_level = logical` in `postgresql.conf`. This enables logical replication. Requires a server restart.

```conf
wal_level = logical
```

### Replication slot and WAL sender limits

Configure PostgreSQL to allow enough replication slots and senders for your deployment:

```conf
max_replication_slots = 10
max_wal_senders = 10
```

Size these based on your deployment:

- `max_replication_slots`: Number of concurrent replication slots across all consumers (each walbox consumer needs one slot)
- `max_wal_senders`: Must be at least as large as `max_replication_slots`

Example: If you run 5 walbox consumers, set both to at least 5. Add headroom for other consumers.

### Role privileges

The connecting role needs `REPLICATION` privilege to use the replication protocol:

```sql
ALTER ROLE consumer_role REPLICATION;
-- or when creating:
CREATE ROLE consumer_role WITH REPLICATION LOGIN PASSWORD '...';
```

### `pg_hba.conf`

Add an entry granting the role access to the `replication` pseudo-database:

```
host replication consumer_role 10.0.0.0/8 scram-sha-256
```

### Connection security

Use `sslmode=require` (or stricter, if your PostgreSQL enforces it) on the replication DSN. This connection carries every row your handler sees, so treat it like any other production database connection, not an internal detail.

Keep the connecting role scoped to `REPLICATION` plus read access to the tables you publish. Don't reuse an application superuser for it.

Load the DSN from an environment variable or your secrets manager. Don't hardcode credentials in source.

## Managed PostgreSQL

Managed providers (Amazon RDS/Aurora, Google Cloud SQL, Azure Database for PostgreSQL) all support logical replication, but none let you edit `postgresql.conf` directly.

- **Enabling `wal_level = logical`**: done through the provider's own mechanism (a parameter group, database flag, or server parameter, depending on the provider). Most still require a reboot to take effect, same as a self-managed instance.
- **Role privileges**: managed providers commonly restrict superuser access, so you can't always run `ALTER ROLE ... REPLICATION` directly. The provider's admin role usually has a scoped equivalent instead (for example, RDS's `rds_replication` role). Check your provider's current documentation for the exact grant.
- **`pg_hba.conf`**: usually managed by the provider too. Look for a security group, firewall rule, or connection policy setting instead of an editable file.

Once replication is enabled, the rest of this page (publication, outbox table, `REPLICA IDENTITY`) is the same regardless of where PostgreSQL runs.

## What you need to create

### Publication

Create a publication that includes the tables walbox should consume:

```sql
CREATE PUBLICATION walbox_pub FOR TABLE outbox;
```

**You must create this manually.** walbox will never create or alter the publication.

If you add tables later:

```sql
ALTER PUBLICATION walbox_pub ADD TABLE another_table;
```

### Outbox table

Create your outbox table with whatever schema your application needs. Example:

```sql
CREATE TABLE outbox (
    id          BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

### REPLICA IDENTITY (for UPDATE/DELETE)

If you only INSERT into the outbox table, `REPLICA IDENTITY` is not required.

If you UPDATE or DELETE rows, the table needs a usable `REPLICA IDENTITY` so PostgreSQL can identify which rows changed:

- **With a primary key (common)**: PostgreSQL automatically uses `REPLICA IDENTITY DEFAULT`, which is sufficient
- **Without a primary key (rare)**: Set explicitly:

  ```sql
  ALTER TABLE outbox REPLICA IDENTITY FULL;
  ```

`REPLICA IDENTITY FULL` captures the entire old row on every update/delete, which is slower than a primary key. Use it only if you can't add a primary key.

## What walbox creates

When you call `await client.run()`:

1. walbox creates the replication slot (idempotently):

   ```sql
   CREATE_REPLICATION_SLOT slot_name LOGICAL pgoutput
   ```

2. If the slot already exists, it's reused

3. walbox does **not** perform any COPY or initial snapshot

On restart, walbox resumes from the last saved checkpoint, not from zero.

## Replication slots and WAL retention

!!! warning "WAL retention"
    Each replication slot tracks how much WAL PostgreSQL must retain. If a walbox consumer is stopped, crashed, or delayed, its slot will cause PostgreSQL to retain WAL, potentially consuming disk space.

    Monitor replication lag and stopped consumers. If a slot is no longer needed, drop it:
    ```sql
    SELECT pg_drop_replication_slot('slot_name');
    ```

## Resource sizing

A walbox consumer is primarily **I/O-bound**. Resource usage depends on your workload and handler implementation.

### Factors that determine resource usage

**Memory**:

- `max_pending_transactions` queue size times typical transaction size
- Large streamed transactions can consume substantial memory during assembly, independent of queue size (see [Monitoring & Backpressure](monitoring.md))
- Handler buffering, if your handler buffers transactions before sending
- Python runtime overhead

**CPU**:

- Single-threaded asyncio event loop; low CPU in typical workloads
- Handler complexity: a compute-heavy handler raises this
- Broker or external I/O is non-blocking, so it doesn't add CPU

**Network**:

- One long-lived PostgreSQL replication connection
- Checkpoint connections (or pool reuse via `build_with_pool()`)
- Handler's own connections to sinks or brokers, which are your responsibility to manage
- Latency matters more than bandwidth for replication

**Disk**:

- Checkpoint frequency affects write rate

### Capacity planning

Start by measuring your actual workload:

1. Run a consumer against production or production-like data
2. Monitor `queue_depth`, `last_handler_latency_seconds`, `replication_lag_bytes`, and process memory
3. Identify the bottleneck: handler latency, broker latency, or memory
4. Adjust accordingly:
   - Slow handler: optimize the business logic, add resources, or add more consumers
   - Memory pressure: reduce `max_pending_transactions`, reduce handler buffering, or move to a larger instance
   - High lag: check whether PostgreSQL is backpressuring, or whether network latency to your broker is the problem

Don't guess resource limits. Observe your workload and set limits with headroom.

## Running walbox

walbox is a Python application. It requires Python 3.13+, Psycopg 3 (included when you `pip install walbox`), and whatever dependencies your handler needs.

Configure your process manager (systemd, Docker, Kubernetes, or similar) to:

- Restart on failure
- Send SIGTERM for graceful shutdown (see [Architecture](architecture.md) for what happens when walbox receives one)
- Allow enough time for the handler to finish before a forceful kill. Shutdown time depends on how long your handler takes to complete: if it takes 30 seconds, shutdown takes about 30 seconds. For Kubernetes, size `terminationGracePeriodSeconds` accordingly:

  ```yaml
  spec:
    terminationGracePeriodSeconds: 30
  ```

  If the handler is hung and the grace period runs out, the container gets killed forcibly. That's no different from a crash: the in-flight transaction is redelivered on restart.
- Wire metrics through the `on_metrics` callback (see [Monitoring & Backpressure](monitoring.md))

## Multi-consumer deployments

To scale horizontally, run multiple walbox consumers, each with:

- Its own replication slot (distinct `slot_name`)
- Its own checkpoint store (distinct `consumer_name`, or a separate store instance)
- A disjoint subset of the data, for example rows where `entity_id % num_consumers == consumer_id`

```python
# Consumer A: processes even entity_ids
client_a = Walbox.build(WalboxOptions(
    consumer_name="consumer-a",
    dsn=dsn,
    slot_name="slot-a",
    publication_name="walbox_pub",
))

# Consumer B: processes odd entity_ids
client_b = Walbox.build(WalboxOptions(
    consumer_name="consumer-b",
    dsn=dsn,
    slot_name="slot-b",
    publication_name="walbox_pub",
))
```

Each consumer reconnects independently, keeps its own checkpoint, and doesn't interfere with the others. Within a consumer, transactions stay in order. Across consumers, there's no guaranteed order relative to each other, which is usually fine for event processing.

Each slot also retains its own WAL independently, so a slow or crashed consumer only affects its own slot's retention, not the others' (see [Replication slots and WAL retention](#replication-slots-and-wal-retention) above).

Budget PostgreSQL connections accordingly. `max_connections` defaults to 100. For example, 5 consumers each with a pool of 5 checkpoint connections:

```
5 consumers * (1 replication + 5 pooled) = 30 connections
+ application servers' connections        = 20
+ superuser headroom                      =  5
+ buffer                                  = 45
= 100 total
```

Leave headroom. Don't fill `max_connections` to the brim.

For finer-grained concurrency within a single consumer, use application-level sharding. See [`examples/concurrency.py`](https://github.com/mochams/walbox/blob/main/examples/concurrency.py) for a pattern that shards rows within one consumer using bounded queues per shard.

## PgBouncer

Needs pgbouncer 1.23.0 or later, in `session` or `transaction` pool_mode. `statement` pool_mode doesn't work: it can't run a block of statements, which both the replication stream and the checkpoint store need.

You must ensure `max_prepared_statements` is not `0`; it defaults to `200` on a current pgbouncer.

## Troubleshooting

**"role is not a member of the replication role"**: The connecting role doesn't have `REPLICATION` privilege. Run `ALTER ROLE consumer_role REPLICATION;` and reconnect.

**"replication slot does not exist"**: If walbox fails to create the slot, check that the role has `REPLICATION` privilege and that `max_replication_slots` has not been exceeded.

**"permission denied"**: Ensure the `pg_hba.conf` entry allows the role to connect for replication.
