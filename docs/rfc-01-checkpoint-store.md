# RFC 01: Checkpoint Store

**Status:** Implemented
**Documented:** 2026-08-23

## Depends on

- ARCHITECTURE.md (error hierarchy: `CheckpointError`; the correctness invariant this
  feature exists to uphold: never tell PostgreSQL a transaction is durably processed
  before the application's own effect and the checkpoint are both durable).

Client Runtime (RFC 05) and Backpressure (RFC 06) both call into this feature, but
neither is a prerequisite for it. `CheckpointStore` is a leaf dependency the rest of
the system builds on, not the other way around.

## Summary / Context

**Problem.** walbox's core promise is at-least-once transaction delivery: a
transaction is redelivered after any crash unless the application has durably
recorded that it already processed it. Without a checkpoint, "durably recorded" has
nowhere to live: a restart would have no way to know where to resume other than
replaying a slot's entire retained history, or worse, trusting PostgreSQL's own
`confirmed_flush_lsn` as if it were the application's own record of success (it
isn't: PostgreSQL only knows the client *acknowledged* a position, not that the
application's handler actually finished with it).

**Business value.** A durable, local checkpoint is what lets an outbox consumer
recover automatically after a crash or restart (no operator intervention, no manual
replay) while still guaranteeing nothing already durably handled gets silently
skipped. It's also the single piece of state that unlocks atomic effect-plus-progress
semantics: when the downstream sink is itself PostgreSQL, the checkpoint update can
join the same transaction as the sink write, which is the closest walbox comes to
exactly-once effects without ever claiming to implement exactly-once delivery itself.

## Goals and Non-Goals

**Goals:**
- A minimal `CheckpointStore` Protocol (`load`/`save`) that any durable backend can
  implement.
- A crash-safe, disk-backed implementation (`FileCheckpointStore`) with no external
  dependencies.
- A Postgres-backed implementation (`PostgresCheckpointStore`) that can execute its
  update *inside a caller-supplied, already-open transaction*, so an application's
  own downstream Postgres write and the checkpoint update commit atomically together.
- A `CheckpointHandle` wrapper so a handler can call `tx.checkpoint.save(...)`
  without needing a reference to the whole client or store.

**Non-Goals:**
- Connection pooling for `PostgresCheckpointStore`. Each standalone call (`load()`,
  or `save()` with no `connection=` given) opens one ad hoc connection, uses it, and
  closes it. A pooled variant is a possible future performance improvement, not
  built here.
- Retry/backoff on transient connection failure. A checkpoint store raises; deciding
  whether that's worth retrying is the client's job (Client Runtime, RFC 05), not the
  store's.
- Defending against a caller `save()`-ing a smaller LSN than what's already stored.
  The client only ever calls `save()` with the commit LSN of transactions it hands
  out in commit order (Transaction Assembly's ordering guarantee, RFC 03), so a
  legitimate caller cannot produce a regressing value; nothing here guards against a
  scenario that can't occur through the documented API.
- Schema migrations for `PostgresCheckpointStore`. The checkpoint table has one
  deliberately minimal shape and no versioning/migration story for v0.1.
- Multi-consumer coordination beyond per-`consumer_name` row/file keying (no leader
  election, no advisory locks, no distributed locking for `FileCheckpointStore`). Two
  processes sharing one `consumer_name`, or one file path, is a misconfiguration
  neither store attempts to detect.
- Connection pooling, retry, or async offload beyond `asyncio.to_thread` for
  `FileCheckpointStore`'s filesystem calls.

## Proposed Design

### The Protocol

```python
class CheckpointStore(Protocol):
    async def load(self) -> int | None: ...
    async def save(self, lsn: int, *, connection: AsyncConnection[Any] | None = None) -> None: ...
```

Async even for a file-backed store, decided the first time a real implementation
existed rather than earlier when there was nothing to implement it against. Async is
the right shape because `PostgresCheckpointStore` must later execute its checkpoint
update *inside the caller's own already-open Postgres transaction*, and an
`AsyncConnection` is event-loop-bound. It cannot be handed into a worker thread the
way a sync-plus-`run_in_executor` design would require. Async is a strict superset at
negligible cost for the file store (it just wraps its blocking calls in
`asyncio.to_thread`); the reverse migration, sync to async later, would break every
existing implementor instead. The optional `connection=` keyword is how a caller's
own cursor joins in: `FileCheckpointStore` ignores it outright (a file can never
participate in a Postgres transaction); `PostgresCheckpointStore` is the
implementation that actually uses it.

### `CheckpointHandle` and `Transaction.checkpoint`

```python
@dataclass(frozen=True, slots=True)
class CheckpointHandle:
    _store: CheckpointStore
    _on_saved: Callable[[int], None] | None = None

    async def save(self, lsn: int, *, connection: AsyncConnection[Any] | None = None) -> None:
        await self._store.save(lsn, connection=connection)
        if self._on_saved is not None:
            self._on_saved(lsn)
```

`Transaction.checkpoint: CheckpointHandle | None = None` lets `TransactionAssembler`
construct `Transaction` objects with zero knowledge of a `CheckpointStore` at all. It
is a pure state machine with no I/O dependencies, deliberately. The client attaches a
real handle before the transaction ever reaches the application's handler, using
`dataclasses.replace`; by the time application code sees a `Transaction`, `checkpoint`
is always populated.

`_on_saved` is a plain, narrowly-scoped callback with one job: notify whoever
constructed this handle that a save just durably completed. This exists because
`ReplicationOptions.manage_checkpoint=False` lets the *application* call
`tx.checkpoint.save(...)` directly, bypassing any client code. Without a hook, the
client would have no way to learn that progress happened in that mode at all, and
would report a stale feedback floor to PostgreSQL forever regardless of how much
work the application actually completed. Routing every `save()` (client-managed or
application-managed) through this one method means feedback (RFC 05) always
reflects reality with no special-casing per mode.

### `FileCheckpointStore`

```python
class FileCheckpointStore:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    async def load(self) -> int | None:
        return await asyncio.to_thread(self._load_sync)

    def _load_sync(self) -> int | None:
        try:
            text = self._path.read_text()
        except FileNotFoundError:
            return None
        return int(text.strip())

    async def save(self, lsn: int, *, connection: AsyncConnection[Any] | None = None) -> None:
        await asyncio.to_thread(self._save_sync, lsn)

    def _save_sync(self, lsn: int) -> None:
        tmp_path = self._path.with_name(self._path.name + ".tmp")
        with open(tmp_path, "w") as f:
            f.write(str(lsn))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, self._path)  # atomic on POSIX
        dir_fd = os.open(self._path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)  # durably persist the rename itself, not just the bytes
        finally:
            os.close(dir_fd)
```

Standard write-to-temp-file, fsync, atomic-rename. `load()` returning `None` for a
missing file is the same "no checkpoint yet, start from the beginning" signal the
client already treats `None` as. The directory `fsync` after `os.replace` is not
decorative: without it, a power loss immediately after the rename can, on some
filesystems, leave the directory entry still pointing at the old inode even though
the rename call itself returned. Fsyncing the containing directory is what makes
the rename itself durable, not just the new file's contents.

### `PostgresCheckpointStore`

```sql
CREATE TABLE IF NOT EXISTS walbox_checkpoint (
    consumer_name TEXT PRIMARY KEY,
    lsn           BIGINT NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`BIGINT` (signed 64-bit) is sufficient: LSNs are conceptually unsigned 64-bit values
but never approach that range in practice. Schema is ensured idempotently
(`CREATE TABLE IF NOT EXISTS`), lazily, on the store's **own** connection the first
time `load()` runs, never inside a caller-supplied `connection=` for `save()`,
since running DDL inside whatever transaction the caller happens to have open would
be surprising and could take unexpected locks. The client always calls `load()` once
at startup, before any `save()` can happen, so `save(..., connection=given)` can
safely assume the table already exists.

```python
async def save(self, lsn: int, *, connection: AsyncConnection[Any] | None = None) -> None:
    if connection is not None:
        await self._upsert(connection, lsn)
        return  # caller owns the transaction boundary -- do NOT commit here

    async with await psycopg.AsyncConnection.connect(self._dsn) as conn:
        await self._ensure_schema(conn)
        await self._upsert(conn, lsn)
        await conn.commit()
```

The `connection is not None` branch is the entire reason this store exists: it never
calls `commit()`. When the application does:

```python
async def handle(tx: Transaction) -> None:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        await conn.execute("INSERT INTO downstream_table (...) VALUES (...)", (...))
        await tx.checkpoint.save(tx.commit_lsn, connection=conn)
        await conn.commit()
```

the sink write and the checkpoint update land in the same transaction and become
durable atomically. Either both survive a crash or neither does. If `save()`
committed internally regardless of `connection=`, this guarantee would be silently
broken.

The table name is a trusted, developer-supplied identifier composed via
`psycopg.sql.Identifier`/`sql.SQL(...).format(...)` rather than an f-string, since
Postgres has no way to bind a table name as a bind parameter and a plain f-string
would silently mis-quote a name with special characters.

## Pros / Cons

**Async Protocol from the start, vs. sync-plus-executor.** Chosen because
`PostgresCheckpointStore`'s whole reason to exist, joining a caller's open
transaction, is impossible if the Protocol is sync and a store is expected to run
its I/O in a worker thread; `AsyncConnection` objects aren't thread-transferable.
Cost: `FileCheckpointStore`, which has no real need to be async, pays one
`asyncio.to_thread` indirection per call. Judged worth it to avoid a breaking
Protocol change later.

**Atomic write-temp-fsync-rename, vs. a simple direct write.** A direct
`path.write_text(str(lsn))` is simpler and almost always fine, but "almost always" is
the wrong bar for the one piece of state at-least-once delivery depends on: a
crash mid-write could leave a truncated or corrupted file, silently breaking resume.
The atomic-rename pattern costs a few extra syscalls per checkpoint in exchange for
"a crash during `save()` never corrupts the previous durable value," which is worth
paying for on every single call, not just the rare one that crashes.

**`save()` never commits when given a `connection=`, vs. always committing for
consistency.** Always committing would be simpler and more uniform across both call
shapes, but it would silently defeat the one feature `PostgresCheckpointStore` exists
to provide: atomic effect-plus-checkpoint. The inconsistency (commits sometimes,
never commits other times) is deliberate and load-bearing, not an oversight.

**No connection pooling for `PostgresCheckpointStore`.** A pool would reduce
per-`save()`/`load()` connection overhead, but checkpoints are inherently
low-frequency (once per transaction at most, often much less under
`manage_checkpoint=False` batching), so the overhead was judged not worth the
added complexity and dependency surface for v0.1.

## Implementation

- `walbox/abc.py`: the async `CheckpointStore` Protocol, `CheckpointHandle`,
  `Transaction.checkpoint`.
- `walbox/checkpoint.py`: `FileCheckpointStore`, `PostgresCheckpointStore`.
- `walbox/client.py`: attaches a `CheckpointHandle` to each assembled `Transaction`
  before calling the handler; auto-saves when `manage_checkpoint=True` (see Client
  Runtime, RFC 05, for the call site).
- `pyproject.toml`: `tool.coverage.run.concurrency` needed `"thread"` added
  alongside `"multiprocessing"`, or coverage.py never instruments code running
  inside `asyncio.to_thread`'s worker threads, leaving `FileCheckpointStore`'s
  actual read/write bodies permanently reported as uncovered regardless of test
  quality.

## Testing

- Round-tripping a value through `save()` then `load()` returns exactly what was
  saved; saving twice with different values leaves only the latest.
- A missing file (or, for Postgres, an unknown `consumer_name`) reports "no
  checkpoint yet" via `None`, not an error.
- `FileCheckpointStore.save` tolerates and ignores an arbitrary `connection=`
  argument: a file can never join a Postgres transaction, so the keyword is a
  no-op there by design, not an oversight.
- Crash-safety, the load-bearing case: a failure injected partway through a second
  `save()` call (e.g. `os.replace`/`os.fsync` raising) leaves the *previous*
  successfully-saved value intact on the next `load()`: a failed write must never
  corrupt or silently advance the durable value.
- `PostgresCheckpointStore.save(lsn, connection=conn)` commits atomically with the
  caller's own transaction: rolling back the caller's transaction rolls back the
  checkpoint update too, proving the store joined the caller's transaction rather
  than committing independently: the single most important correctness property
  this store has.
- `PostgresCheckpointStore.save` with a `connection=` never calls `commit()` itself
  (spied directly), confirming the atomicity guarantee isn't accidentally satisfied
  by some other code path.
- Two stores sharing one table but different `consumer_name`s never clobber each
  other's rows; repeated saves for the same `consumer_name` upsert one row rather
  than accumulating duplicates.
- Pointing `PostgresCheckpointStore` at a database where its table doesn't exist yet,
  `load()` creates it lazily and returns `None` without error, proving the
  idempotent, load()-time schema creation actually works standalone.
