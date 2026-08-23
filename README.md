# walbox

Async Python library for consuming PostgreSQL logical replication as a stream of
committed transactions, built for the transactional outbox pattern: write an outbox
row in the same transaction as your business data, then stream committed outbox
inserts to an external system without polling the table and without `LISTEN`/`NOTIFY`.

## Status

walbox is pre-1.0. The nine names in `walbox.__all__` —
`ReplicationClient`, `ReplicationOptions`, `Transaction`, `ChangeEvent`, `ChangeKind`,
`CheckpointStore`, `FileCheckpointStore`, `PostgresCheckpointStore`, `WalboxError` — are
the stable v0.1 surface: this is what `from walbox import ...` gives you, and what
future releases commit to keeping around. Their construction signatures and field names
may still change between minor pre-1.0 releases, but the *set* of what's exported is
settled. Specific error subclasses (`walbox.errors.ProtocolError`,
`walbox.errors.DecodeError`, `walbox.errors.ReplicationConnectionError`,
`walbox.errors.CheckpointError`) stay importable from `walbox.errors` directly and are
not part of the top-level surface.

The `Metrics` callback shape and the exact semantics of `Transaction` for streamed vs.
non-streamed sources are provisional and more likely to evolve before 1.0 than the rest
of the surface above.

Once 1.0 ships, changes to the stable surface will follow semver. Nothing before 1.0 is
guaranteed stable release-to-release.

## Install

For production, install plain `psycopg`, which requires `libpq` available at build/run
time (typically a system package, e.g. `libpq-dev` on Debian/Ubuntu or `postgresql-libs`
on Alpine):

```sh
pip install walbox
```

To try walbox locally without installing a system `libpq`, add psycopg's self-contained
binary wheel alongside it:

```sh
pip install walbox "psycopg[binary]"
```

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

A complete, runnable version of this example — including the same-transaction
checkpoint pattern for a Postgres sink, see "Exactly-once effects" below — lives in
[`examples/outbox.py`](examples/outbox.py).

## PostgreSQL configuration

- `wal_level = logical` in `postgresql.conf`. Requires a server restart to take effect.
- Size `max_replication_slots` and `max_wal_senders` with headroom for at least one
  slot/sender per walbox consumer. `max_wal_senders` should be at least as large as
  `max_replication_slots`, since each active logical slot occupies a walsender process
  while a consumer is connected.
- The connecting role needs the `REPLICATION` attribute:

  ```sql
  ALTER ROLE consumer_role REPLICATION;
  -- or, at creation time:
  CREATE ROLE consumer_role WITH REPLICATION LOGIN;
  ```

- A `pg_hba.conf` entry granting that role access to the special `replication`
  pseudo-database, e.g.:

  ```text
  host replication consumer_role 10.0.0.0/8 scram-sha-256
  ```

- On PostgreSQL 15+, the connecting role additionally needs `SELECT` privilege on the
  published tables — PostgreSQL 15 tightened this requirement. On 14 it is not enforced.
- The published table needs a usable `REPLICA IDENTITY` for `UPDATE`/`DELETE` decoding
  to see old-row data at all. A primary key (the `DEFAULT` case) is sufficient for most
  outbox-style tables. A table with no primary key needs an explicit
  `ALTER TABLE ... REPLICA IDENTITY FULL` (or `USING INDEX`), or `UPDATE`/`DELETE` on it
  will replicate with no old-row information.
- walbox creates its replication slot idempotently if missing. It does **not** create
  the publication — `CREATE PUBLICATION` is a manual, one-time operational step (also
  listed under Limitations, below).

## Failure semantics

| Crash point | Outcome on restart |
|---|---|
| Before handler runs | Transaction fully redelivered once replication resumes from the durable checkpoint. |
| During handler execution | Redelivered in full; sink must tolerate a partially-applied-then-repeated attempt (dedupe on `outbox.id`). |
| After handler succeeds, before checkpoint saved | Redelivered — the canonical at-least-once duplicate. This is intentional; durability is never sacrificed to avoid it. |
| During checkpoint save | The previous durable checkpoint remains valid (`FileCheckpointStore`'s atomic rename, or `PostgresCheckpointStore`'s transactional rollback leaving the prior committed row intact); transaction redelivered. |
| After checkpoint durable, before feedback sent | Feedback resumes from the checkpoint on reconnect; Postgres may redeliver already-checkpointed work if its own `confirmed_flush_lsn` lagged — tolerated, handler must stay idempotent regardless. |
| After feedback sent, before Postgres durably records it | Same as above — feedback is a hint for WAL retention/restart position, never the app's own source of truth for progress. |
| Mid-receive (partway through a WAL message) | The partial message is never assembled into a `Transaction`; full retransmission from the checkpoint on reconnect. |
| Mid-keepalive reply | No durable state was claimed; safe — the connection simply times out and reconnects normally. |
| Mid-reconnect | Next attempt retries from the same durable checkpoint; a failed reconnect attempt claims no progress. |
| Mid-shutdown | Never worse than a plain crash at the same point in the sequence — whatever wasn't yet durable is redelivered, whatever was, isn't. |
| Mid-large/streamed transaction | The in-memory streamed-transaction buffer is lost — it was never durable by design. Postgres resends the entire transaction from scratch on reconnect; no partial/corrupt delivery. |

## Exactly-once effects

walbox provides **at-least-once transaction delivery** with a durable replay position.
It does **not** implement or claim end-to-end exactly-once effects, and never will
inside the replication reader itself.

The flow: Postgres transaction → outbox row → logical replication → handler → external
sink. Exactly-once *effects* are achieved by combining PostgreSQL's transactional
outbox write with durable checkpointing, replay-after-failure, and an idempotent or
deduplicating sink — concretely, either:

- deduplicating on `outbox.id` (the natural event ID) at the sink, or
- using a native transactional/idempotent mechanism, such as `PostgresCheckpointStore`'s
  same-transaction pattern, when the sink is itself PostgreSQL:

  ```py
  async def handle_with_atomic_checkpoint(tx: Transaction, dsn: str) -> None:
      async with await psycopg.AsyncConnection.connect(dsn) as conn:
          # Your own downstream write(s) go here, iterating tx.changes the
          # same way handle() does above. They must run on `conn`,
          # uncommitted, so they become durable together with the
          # checkpoint below -- never on a separate connection/transaction.

          await tx.checkpoint.save(tx.commit_lsn, connection=conn)
          await conn.commit()
  ```

  (same code as `handle_with_atomic_checkpoint` in
  [`examples/outbox.py`](examples/outbox.py))

If the process crashes after an external publish succeeds but before the checkpoint is
durable, the transaction **will** be delivered again. This is intentional and desirable,
not a bug to be minimized.

## Supported versions

- **PostgreSQL 14+.** This is the floor for protocol version 2 / `streaming 'on'`,
  which walbox always negotiates. There is no attempt at graceful degradation to
  protocol version 1 on older servers — connecting to a pre-14 server is unsupported,
  not silently degraded.
- **Python 3.13+.** `asyncio.Queue.shutdown()`, which walbox's bounded-backpressure and
  graceful-shutdown handling depends on, is a 3.13 addition.

Both floors are deliberate choices for v0.1, not aspirations to relax later.

## Limitations

- Streamed-transaction memory is not accounted against `max_pending_transactions` /
  the bounded queue's own memory bound — a large, or a large number of concurrent,
  streamed transactions can grow process memory independent of that configured bound.
- No built-in metrics exporter or framework — only a synchronous `on_metrics` callback;
  wiring it to Prometheus/StatsD/etc. is left to the application.
- Strictly sequential, single-consumer handling — no concurrent handler execution.
  Throughput is bounded by one handler invocation at a time.
- Manual publication management — walbox creates its replication slot idempotently but
  never creates or alters the publication; that is an operational step the
  application/operator owns.
- Truncate's `CASCADE`/`RESTART IDENTITY` flags are decoded but not surfaced on
  `ChangeEvent` — the application sees that a truncate happened and on which table, not
  the flags it was issued with. Type and Origin messages are parsed but never surfaced
  to the application at all.
