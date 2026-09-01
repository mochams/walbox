# Upgrading

walbox went through three pre-release versions before v1.0.0: `beta`, `beta.1`, `beta.2`, and `rc.1`. Sections below are ordered newest first. Find the version you're upgrading from and read down to the bottom; each section is cumulative.

See [`CHANGELOG.md`](https://github.com/mochama/walbox/blob/main/CHANGELOG.md) for the complete, unedited history.

## `rc.1` → `1.0.0`

No breaking changes. The public API is unchanged from `rc.1`; this release is documentation and example additions (an FAQ, this Upgrading guide, a metrics example). See the [Unreleased section of the CHANGELOG](https://github.com/mochama/walbox/blob/main/CHANGELOG.md#unreleased) for the full list.

## `beta.2` → `rc.1`

This is the version with breaking changes. If you're on `beta.2` or earlier, this is the section that affects your code.

### `ReplicationOptions` and `ReplicationClient` are gone

`WalboxOptions` is now the only options type, and `Client` replaces `ReplicationClient`.

**Before:**

```python
from walbox import ReplicationOptions, ReplicationClient, PostgresCheckpointStore

options = ReplicationOptions(...)
checkpoint_store = PostgresCheckpointStore(...)
client = ReplicationClient(options, checkpoint_store)
```

**After:**

```python
from walbox import WalboxOptions, Walbox

options = WalboxOptions(...)
client = Walbox.build(options)
```

### `FileCheckpointStore` is removed

PostgreSQL is the only supported checkpoint backend. `Walbox.build()` and `Walbox.build_with_pool()` construct the `PostgresCheckpointStore` for you, so you don't need to name it directly.

**Before:**

```python
from walbox import FileCheckpointStore

checkpoint_store = FileCheckpointStore(path="/var/lib/walbox/checkpoint")
```

**After:**

```python
from walbox import Walbox

client = Walbox.build(options)
# or, with a pooled connection for checkpoint saves:
client = Walbox.build_with_pool(options, pool)
```

### `build_with_pool()` takes your own connection pool

Pass a `psycopg_pool.AsyncConnectionPool` you created directly. You open and close it; walbox only uses it.

```python
from psycopg_pool import AsyncConnectionPool
from walbox import Walbox

async with AsyncConnectionPool(dsn, min_size=1, max_size=5) as pool:
    client = Walbox.build_with_pool(options, pool)
    await client.run(handle)
```

### Top-level exports changed

Removed from `walbox`: `CheckpointStore`, `PostgresCheckpointStore`, `ReplicationOptions`, `FileCheckpointStore`.

Added: `WalboxOptions`, `Walbox`, `ConnectionPool`.

If you were constructing `PostgresCheckpointStore` or implementing `CheckpointStore` directly, those are still available, just not at the top level: import from `walbox.checkpoint` and `walbox.abc` instead.

### Everything else

`CheckpointHandle.save()` now rejects an LSN past the transaction's commit LSN, and `WalboxOptions` validates its fields at construction time. See the [rc.1 CHANGELOG entry](https://github.com/mochams/walbox/blob/main/CHANGELOG.md#100-rc1---2026-08-29) for the full list.

## `beta.1` → `beta.2`

No breaking changes.

- **Added** `Metrics.transactions_since_checkpoint`, tracking how many transactions have been processed since the last durable checkpoint save.
- **Added** `examples/outbox_concurrency.py`, a sharded, order-preserving concurrent handler pattern.

## `beta` → `beta.1`

No breaking changes.

- **Added** `PostgresCheckpointStore.from_pool()`, letting the checkpoint store reuse an application-managed `psycopg_pool.AsyncConnectionPool` instead of opening a new connection per call.
- **Added** `examples/outbox_pool.py` and `examples/outbox_postgres_pool.py`, demonstrating the pooled checkpoint store.
