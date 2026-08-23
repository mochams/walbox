# walbox

[![PyPI](https://img.shields.io/pypi/v/walbox.svg)](https://pypi.org/project/walbox/)
[![CI](https://github.com/mochams/walbox/actions/workflows/ci.yml/badge.svg)](https://github.com/mochams/walbox/actions/workflows/ci.yml)
[![Python 3.13+](https://img.shields.io/badge/python-3.13%2B-blue.svg)](https://www.python.org/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Async Python runtime for consuming PostgreSQL logical replication as a stream of
committed transactions, built for the transactional outbox pattern: write an outbox
row in the same transaction as your business data, then stream committed inserts to
an external system with no polling and no `LISTEN`/`NOTIFY`.

- **At-least-once delivery**: a durable local checkpoint, never silent loss
- **Backpressure-aware**: a slow handler can't blow up memory or starve PostgreSQL's keepalives
- **Reconnects and resumes** automatically from the last durable checkpoint
- **Graceful shutdown**: finishes in-flight work and checkpoints it before exiting
- **Asyncio-native**, one dependency (`psycopg`)

## Install

```sh
pip install walbox
```

Requires `libpq` available at build/run time (typically a system package, e.g.
`libpq-dev` on Debian/Ubuntu). To try walbox without a system `libpq`, install
psycopg's self-contained wheel alongside it: `pip install walbox "psycopg[binary]"`.

## Quickstart

```sql
-- Run once, manually. walbox creates its replication slot idempotently, but
-- never creates or alters the publication itself (see "PostgreSQL configuration"
-- below).
CREATE TABLE outbox (
    id          BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE PUBLICATION walbox_pub FOR TABLE outbox;
```

```py
import asyncio
import signal

from walbox import (
    ChangeKind,
    PostgresCheckpointStore,
    ReplicationClient,
    ReplicationOptions,
    Transaction,
)


async def publish_to_broker(payload: dict) -> None:
    # Replace with your actual publish call.
    print("publishing:", payload)


async def handle(tx: Transaction) -> None:
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue
        await publish_to_broker(change.new)

    await tx.checkpoint.save(tx.commit_lsn)


async def main() -> None:
    dsn = "your-postgres-dsn"
    checkpoint_store = PostgresCheckpointStore(dsn, consumer_name="my-consumer")

    options = ReplicationOptions(
        consumer_name="my-consumer",
        dsn=dsn,
        slot_name="outbox_slot",
        publication_name="walbox_pub",
        checkpoint_store=checkpoint_store,
        manage_checkpoint=False,  # handle() checkpoints explicitly, after publishing.
    )

    client = ReplicationClient(options)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, client.close)

    await client.run(handle)


if __name__ == "__main__":
    asyncio.run(main())
```

A complete, runnable version lives in [`examples/outbox.py`](examples/outbox.py),
including the same-transaction checkpoint pattern for a Postgres sink (see
"Exactly-once effects" below).

## PostgreSQL configuration

- `wal_level = logical` in `postgresql.conf` (requires a server restart).
- Size `max_replication_slots`/`max_wal_senders` with headroom for at least one
  slot/sender per consumer; `max_wal_senders` should be at least as large as
  `max_replication_slots`.
- The connecting role needs `REPLICATION`: `ALTER ROLE consumer_role REPLICATION;`
  (or `CREATE ROLE ... WITH REPLICATION LOGIN;`).
- A `pg_hba.conf` entry granting that role access to the `replication`
  pseudo-database, e.g. `host replication consumer_role 10.0.0.0/8 scram-sha-256`.
- On PostgreSQL 15+, the connecting role additionally needs `SELECT` on the
  published tables (15 tightened this; 14 doesn't enforce it).
- The published table needs a usable `REPLICA IDENTITY` for `UPDATE`/`DELETE` to see
  old-row data. A primary key (`DEFAULT`) is enough for most outbox-style tables; a
  table with none needs `ALTER TABLE ... REPLICA IDENTITY FULL` (or `USING INDEX`).
- walbox creates its replication slot idempotently if missing. It does **not** create
  the publication: `CREATE PUBLICATION` is a manual, one-time step (see Limitations).

## Exactly-once effects

walbox provides **at-least-once delivery** with a durable replay position. It does
**not** implement or claim end-to-end exactly-once effects. The flow: Postgres
transaction → outbox row → logical replication → handler → external sink.
Exactly-once *effects* come from combining the transactional outbox write with
durable checkpointing and an idempotent/deduplicating sink: either dedupe on
`outbox.id`, or, when the sink is itself PostgreSQL, use
`PostgresCheckpointStore`'s same-transaction pattern (`handle_with_atomic_checkpoint`
in [`examples/outbox.py`](examples/outbox.py)). If the process crashes after an
external publish succeeds but before the checkpoint is durable, the transaction
**will** be delivered again. That's intentional, not a bug.

## Failure semantics

walbox is correct if the process crashes at *any* point, whether before, during, or
after the handler runs, mid-checkpoint, mid-reconnect, mid-shutdown, or partway
through a large streamed transaction. The result is always "delivered again" or "not
delivered yet," never silent loss and never a torn transaction. The full
crash-point-by-crash-point table is in
[`ARCHITECTURE.md`](ARCHITECTURE.md#failure-semantics).

## Supported versions

- **PostgreSQL 14+**: the floor for protocol version 2 / `streaming 'on'`, which
  walbox always negotiates. A pre-14 server is unsupported, not silently degraded.
- **Python 3.13+**: `asyncio.Queue.shutdown()`, which backpressure and graceful
  shutdown depend on, is a 3.13 addition.

Both are deliberate v0.1 floors, not aspirations to relax later.

## Limitations

- Streamed-transaction memory isn't accounted against `max_pending_transactions`, so
  large or numerous concurrent streamed transactions can grow memory independent of
  that bound.
- No built-in metrics exporter, only a synchronous `on_metrics` callback; wiring it
  to Prometheus/StatsD/etc. is left to the application.
- Strictly sequential, single-consumer handling, with no concurrent handler execution.
- Manual publication management: walbox never creates or alters the publication.
- Truncate's `CASCADE`/`RESTART IDENTITY` flags, and Type/Origin message content, are
  decoded but never surfaced to the application.

## Status

The code is tested and correct for everything described above: 100% branch coverage,
including integration tests against real PostgreSQL for every failure scenario in the
table above. "Pre-1.0" here is about the API surface still settling, not about whether
it's safe to run.

Concretely: the public export list won't shrink, though construction signatures and
field names may still shift between pre-1.0 releases. The `Metrics` callback shape and
streamed-vs-non-streamed `Transaction` semantics are most likely to still change before
1.0. Once 1.0 ships, the stable surface follows semver.

## See also

- [`ARCHITECTURE.md`](ARCHITECTURE.md): system design, the correctness invariant, the error hierarchy
- [`docs/README.md`](docs/README.md): the RFCs behind each feature
- [`CONTRIBUTING.md`](CONTRIBUTING.md): development setup, tests, code style
- [`PROJECT.md`](PROJECT.md): project status and tooling rationale
- [`LICENSE`](LICENSE): MIT
