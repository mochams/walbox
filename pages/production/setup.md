# PostgreSQL Setup

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

## Operational considerations

### Replication slots and WAL retention

!!! warning "WAL retention"
    Each replication slot tracks how much WAL PostgreSQL must retain. If a walbox consumer is stopped, crashed, or delayed, its slot will cause PostgreSQL to retain WAL, potentially consuming disk space.

    Monitor replication lag and stopped consumers. If a slot is no longer needed, drop it:
    ```sql
    SELECT pg_drop_replication_slot('slot_name');
    ```

### Multi-consumer deployments

Each walbox consumer needs its own:
- Replication slot (distinct slot name)
- Checkpoint store (FileCheckpointStore or PostgresCheckpointStore)

Each slot independently retains WAL from its resume position. If one consumer lags, it doesn't affect others' WAL retention, but it does prevent that WAL from being recycled globally.

## Quick setup checklist

```sql
-- 1. Create the outbox table
CREATE TABLE outbox (
    id          BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 2. Create the publication
CREATE PUBLICATION walbox_pub FOR TABLE outbox;

-- 3. (Optional) Verify REPLICA IDENTITY
SELECT relname, relreplident FROM pg_class WHERE relname = 'outbox';
-- Output: outbox | d (default, if there's a primary key)
```

Configure `postgresql.conf`:

```conf
wal_level = logical
max_replication_slots = 10      # Adjust based on number of consumers
max_wal_senders = 10             # Should match max_replication_slots
```

Restart PostgreSQL, then create the role and `pg_hba.conf` entry:

```sql
CREATE ROLE consumer_role WITH REPLICATION LOGIN PASSWORD '...';
```

In `pg_hba.conf`:

```
host replication consumer_role 10.0.0.0/8 scram-sha-256
```

## Troubleshooting

**"role is not a member of the replication role"**: The connecting role doesn't have `REPLICATION` privilege. Run `ALTER ROLE consumer_role REPLICATION;` and reconnect.

**"replication slot does not exist"**: If walbox fails to create the slot, check that the role has `REPLICATION` privilege and that `max_replication_slots` has not been exceeded.

**"permission denied"**: Ensure the `pg_hba.conf` entry allows the role to connect for replication.
