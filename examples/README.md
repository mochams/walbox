# Examples

Runnable scripts demonstrating walbox patterns. Each one is self-contained and has a module docstring with the exact setup SQL it needs.

## Prerequisites

- A PostgreSQL instance with `wal_level = logical` (see [Setup & Deployment](https://mochams.github.io/walbox/production/setup/))
- `pip install walbox psycopg-pool`
- The `WALBOX_DSN` environment variable set to your connection string

## One-time setup

Run [`table.sql`](table.sql) once, against your database, before running any of the scripts below:

```sh
psql "$WALBOX_DSN" -f table.sql
```

This creates `published_table` and the `walbox_pub` publication that every example reads from.

`postgres.py` needs one more table. Run [`postgres-sink.sql`](postgres-sink.sql) as well before running it:

```sh
psql "$WALBOX_DSN" -f postgres-sink.sql
```

## Running an example

Each script reads `WALBOX_DSN` from the environment and runs until stopped (Ctrl-C or SIGTERM):

```sh
export WALBOX_DSN="postgresql://user:password@localhost/dbname"
python examples/broker.py
```

In another terminal, insert a row to see it flow through:

```sql
INSERT INTO published_table (entity_type, entity_id, event_type, payload)
VALUES ('user', '42', 'created', '{"name": "Alice"}'::jsonb);
```

## The scripts

- **[`broker.py`](broker.py)**: publishes each insert to an external message broker (stubbed here), checkpointing after a successful publish.
- **[`postgres.py`](postgres.py)**: writes to a PostgreSQL sink and saves the checkpoint in the same transaction, for exactly-once effects without external deduplication. See the [PostgreSQL sink guide](https://mochams.github.io/walbox/examples/postgresql/) for the full pattern.
- **[`concurrency.py`](concurrency.py)**: shards one transaction's changes across worker queues keyed by `entity_id`, so different entities process concurrently while each entity's own events stay in order.
- **[`metrics.py`](metrics.py)**: contrasts a blocking `on_metrics` callback (what not to do) with a non-blocking one that hands metrics off to a background task. See the [Monitoring guide](https://mochams.github.io/walbox/production/monitoring/) for why this matters.
