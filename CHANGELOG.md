# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and versioning
follows [Semantic Versioning](https://semver.org/) (pre-1.0 tags use the
`-beta`/`-rc` pre-release suffix).

## [Unreleased]

### Breaking changes

- **`ReplicationOptions` removed.** `WalboxOptions` is now the only options
  type. `Client` takes it directly plus a `CheckpointStore`:
  `Client(options, checkpoint_store)`, instead of embedding the
  checkpoint store inside the options object.
- **`ReplicationClient` renamed to `Client`.**
- **`FileCheckpointStore` removed.** PostgreSQL is now the only supported
  checkpoint backend for the public API.
- **New `Walbox`** is the recommended way to construct a client:
  `Walbox.build(options)` or `Walbox.build_with_pool(options,
  pool)`, replacing manual construction of `PostgresCheckpointStore` +
  `ReplicationOptions` + `ReplicationClient`.
- **`Walbox.build_with_pool()` takes your own connection pool**
  (`build_with_pool(options, pool)`) instead of building one from a
  `PoolOptions` config object (`PoolOptions` removed). You open and close
  the pool; walbox only uses it.
- **Top-level `walbox` export surface changed.** Removed: `CheckpointStore`,
  `PostgresCheckpointStore`, `ReplicationOptions`, `FileCheckpointStore`,
  `PoolOptions`. Added: `WalboxOptions`, `Walbox`, `ConnectionPool`.
  Advanced/manual construction remains available via `walbox.abc` and
  `walbox.checkpoint`.
- **Example scripts consolidated and renamed**, all now built on
  `build_with_pool()`:
  - `outbox.py` + `outbox_pool.py` → `broker.py`
  - `outbox_postgres.py` + `outbox_postgres_pool.py` → `postgres_sink.py`
  - `outbox_concurrency.py` → `concurrency.py`

  The demo table used across the examples is renamed from `outbox` to
  `published_table`, since walbox works with any published table, not just a
  dedicated outbox table.

### Added

- `PostgresCheckpointStore.load()` rejects a negative stored LSN with
  `CheckpointError` instead of silently returning it.
- `CheckpointHandle.save()` rejects an LSN greater than the commit LSN of the
  transaction it was dispatched for, raising `CheckpointError`. This closes
  the one direction that was genuinely unsafe: a handler bug durably
  acknowledging progress walbox never actually made, which could let
  PostgreSQL recycle WAL out from under unprocessed data.
- `WalboxOptions` validates itself at construction time: required strings
  must not be blank, and `max_pending_transactions`/`status_interval` must
  be positive; invalid values raise `ValueError` immediately.
- pgoutput decoding covers `Message` records (from
  `pg_logical_emit_message()`), decoded and logged like `Type`/`Origin`,
  rather than raising `DecodeError` if one is ever encountered.
- Full documentation site (mkdocs): getting started, production deployment,
  delivery guarantees, monitoring, and per-integration example guides (NATS,
  RabbitMQ, Temporal, webhooks, PostgreSQL, transactional outbox).
- Integration coverage for a genuine, uncatchable process kill: `Client` is
  run as a real OS subprocess, sent `SIGKILL`, and a second, independently
  cold-started process is verified to resume correctly from the durable
  checkpoint — redelivering only what was never durably checkpointed, never
  re-delivering what was, and never losing anything in between. Previously
  the reconnect/shutdown tests only covered a dropped connection within the
  same process (`pg_terminate_backend`) or a cooperative `close()`, neither
  of which proves anything about an actual crash.

### Removed

- The project's `docs/rfc-*.md` design-record documents and
  `docs/research/` write-up, superseded by the documentation site.

### Notes

- `Walbox.build()` remains available for low checkpoint volume or
  when avoiding the `psycopg-pool` dependency matters; `build_with_pool()`
  is the recommended default otherwise, since `build()` opens and closes a
  new connection on every `checkpoint.save()` call.

## [1.0.0-beta.2] - 2026-08-24

### Added

- `Metrics.transactions_since_checkpoint`, tracking how many transactions
  have been processed since the last durable checkpoint save.
- `examples/outbox_concurrency.py`: a sharded, order-preserving concurrent
  handler pattern for fanning one transaction's changes across worker
  queues.

### Changed

- Internal cleanup across `walbox/abc.py`, `walbox/checkpoint.py`,
  `walbox/client.py`, and the example scripts.

## [1.0.0-beta.1] - 2026-08-24

### Added

- `PostgresCheckpointStore.from_pool()`, letting the checkpoint store reuse
  an application-managed `psycopg_pool.AsyncConnectionPool` instead of
  opening a new connection per call.
- `examples/outbox_pool.py` and `examples/outbox_postgres_pool.py`,
  demonstrating the pooled checkpoint store.

### Changed

- Simplified example scripts for clarity.

## [1.0.0-beta] - 2026-08-23

Initial pre-1.0 release: an async PostgreSQL logical-replication runtime for
the transactional outbox pattern.

- Replication transport over libpq (COPY BOTH, TLS-transparent).
- Wire decoding: outer replication-message framing plus the pgoutput
  sub-protocol, including streamed (in-progress) transactions.
- Transaction assembly, buffered per-xid, emitted only on commit.
- `PostgresCheckpointStore`-backed durable checkpointing, including the
  same-transaction (`connection=`) atomic-checkpoint pattern.
- Bounded delivery queue for backpressure; reconnect with exponential
  backoff; graceful shutdown on `close()`.
- Metrics via an `on_metrics` callback.
- CI and PyPI publish workflow, pre-commit hooks.
