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
- A `CheckpointHandle` wrapper, handed to every handler call as a second argument,
  so a handler can call `checkpoint.save(...)` without needing a reference to the
  whole client or store.
- An optional way to construct `PostgresCheckpointStore` (`from_pool`) that reuses
  an application-owned connection pool for its own ad hoc `load()`/connection-less
  `save()` calls, instead of always opening a fresh connection.

**Non-Goals:**
- Owning or managing a connection pool's lifecycle. `from_pool` accepts an
  already-constructed pool and only ever checks connections out of and back into
  it; creating, sizing, and closing that pool is the application's job, same as
  `dsn` is just a string the default constructor doesn't own either.
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

### `CheckpointHandle`, handed to the handler directly

```python
@dataclass(frozen=True, slots=True)
class CheckpointHandle:
    _store: CheckpointStore
    _on_saved: Callable[[int, float], None] | None = None

    async def save(self, lsn: int, *, connection: AsyncConnection[Any] | None = None) -> None:
        started_at = time.monotonic()
        await self._store.save(lsn, connection=connection)
        if self._on_saved is not None:
            self._on_saved(lsn, time.monotonic() - started_at)
```

`Handler = Callable[[Transaction, CheckpointHandle], Awaitable[None]]`: the client
passes one `CheckpointHandle`, bound to `options.checkpoint_store`, as the handler's
second argument on every call, rather than attaching it to `Transaction` itself.
`Transaction` stays a pure data value (xid, commit LSN, commit time, changes) with no
knowledge of a `CheckpointStore` at all, constructed by `TransactionAssembler` (a
pure state machine with no I/O dependencies) and never touched afterward; the client
constructs the one `CheckpointHandle` for a run and passes the same instance to every
handler call. This also means every handler is unconditionally responsible for
calling `checkpoint.save(...)` itself -- walbox has no path that checkpoints on a
handler's behalf (see Client Runtime, RFC 05, for why an auto-checkpoint mode was
removed rather than kept as an option).

`_on_saved` is a plain, narrowly-scoped callback with two jobs: notify the client that
a save just durably completed (so feedback, RFC 05, can advance), and report how long
the underlying `store.save()` call took, for the `last_checkpoint_latency_seconds`
metric (Observability, RFC 07). Timing lives here, in `CheckpointHandle.save()`,
rather than in client code wrapping a `save()` call it makes itself, because the
client itself never calls `save()` anymore -- this is the one call site every
`save()`, from any handler, always passes through.

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
(`CREATE TABLE IF NOT EXISTS`), lazily, the first time it's needed on *whichever*
connection `save()`/`load()` is about to use -- including a caller-supplied
`connection=` -- but never committed there directly:

```python
async def _ensure_schema(self, conn: AsyncConnection[Any]) -> None:
    if self._schema_ready:
        return
    await conn.execute("CREATE TABLE IF NOT EXISTS ...")
    self._schema_ready = True  # left uncommitted -- see below

async def save(self, lsn: int, *, connection: AsyncConnection[Any] | None = None) -> None:
    if connection is not None:
        await self._ensure_schema(connection)
        await self._upsert(connection, lsn)
        return  # caller owns the transaction boundary -- do NOT commit here

    async with self._acquire() as conn:
        await self._ensure_schema(conn)
        await self._upsert(conn, lsn)
        await conn.commit()
```

`CREATE TABLE IF NOT EXISTS` is transactional in Postgres, so running it on a
caller-supplied connection and leaving it uncommitted is safe: it becomes durable
together with whatever else the caller commits on that connection, same as the
upsert. This closed a real gap in an earlier version of this store, which only ever
ensured the schema from `load()`'s own connection and skipped it entirely on the
`connection=` path -- correct only because `ReplicationClient.run()` always calls
`load()` once before any handler runs. A `PostgresCheckpointStore` used standalone
(e.g. in the same-transaction pattern, calling `save(lsn, connection=conn)` as its
very first operation with no preceding `load()`) would hit "relation does not exist."
Ensuring the schema inline, on whatever connection is actually in play, removes that
implicit ordering dependency entirely.

The `connection is not None` branch is the entire reason this store exists: it never
calls `commit()`. When the application does:

```python
async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        await conn.execute("INSERT INTO downstream_table (...) VALUES (...)", (...))
        await checkpoint.save(tx.commit_lsn, connection=conn)
        await conn.commit()
```

the sink write and the checkpoint update land in the same transaction and become
durable atomically. Either both survive a crash or neither does. If `save()`
committed internally regardless of `connection=`, this guarantee would be silently
broken.

`self._acquire()` is a zero-argument callable returning an async context manager
that yields one connection -- the indirection `from_pool` needs (see below) to swap
where connection-less `save()`/`load()` calls get their connection from, without
`save()`/`load()`'s own bodies needing to know or care which source is in play.

The table name is a trusted, developer-supplied identifier composed via
`psycopg.sql.Identifier`/`sql.SQL(...).format(...)` rather than an f-string, since
Postgres has no way to bind a table name as a bind parameter and a plain f-string
would silently mis-quote a name with special characters.

### Pooled construction: `PostgresCheckpointStore.from_pool`

```python
class ConnectionPool(Protocol):
    def connection(self) -> AbstractAsyncContextManager[AsyncConnection[Any]]: ...

@classmethod
def from_pool(cls, pool: ConnectionPool, *, consumer_name: str, table: str = "walbox_checkpoint") -> "PostgresCheckpointStore":
    store = cls.__new__(cls)
    store._acquire = pool.connection
    store._configure(consumer_name=consumer_name, table=table)
    return store
```

`ConnectionPool` matches `psycopg_pool.AsyncConnectionPool`'s real shape (a plain
method returning an async context manager that checks a connection out and returns
it to the pool on exit) structurally, without importing `psycopg_pool` -- any object
shaped like this works, including a hand-rolled one, so accepting a pool this way
costs walbox nothing beyond `psycopg` itself. `from_pool` is an alternate
constructor rather than an extra `pool=` keyword on `__init__`: the default
constructor's `dsn` and this constructor's `pool` are mutually exclusive by
construction, so there's nothing to validate at the boundary (no "exactly one of
`dsn`/`pool` must be given" runtime check needed).

The pool is never opened, sized, or closed here -- it's the application's, created
and owned outside this store, exactly like a plain `dsn` string is never owned by
the default constructor either. This only changes where connections for `load()`
and connection-less `save()` calls come from; the same-transaction pattern
(`save(lsn, connection=conn)`) is completely unaffected, since it already uses
whatever connection the caller passes in, pool-backed or not. Conflating the two
would be a real correctness bug: checking a connection out of the pool for a
downstream write and separately letting connection-less `save()` check out a
*different* connection from the same pool would put the write and the checkpoint on
two unrelated connections, silently breaking atomicity.

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

**A structurally-typed `ConnectionPool` accepted via `from_pool`, vs. depending on
`psycopg_pool` directly, vs. still not pooling at all.** Checkpointing stays
inherently low-frequency (once per transaction at most, often much less if an
application batches its own `save()` calls), so the original v0.1 decision not to
pool was reasonable at the time, but it left a cost every plain `save()`/`load()`
call pays regardless of how rarely it's called: a full connect-and-auth round trip
that's simply wasted if the application already maintains a pool for its own use.
Depending on `psycopg_pool` directly would make `from_pool` more discoverable (a
concrete type instead of a `Protocol`), but costs walbox its one-dependency
footprint for a feature most consumers won't use. A structural `Protocol` gets the
overhead reduction with no new dependency, at the cost of a slightly less
discoverable type signature (`ConnectionPool`, not `psycopg_pool.AsyncConnectionPool`
directly) -- judged the better trade given how deliberately small walbox's
dependency surface has stayed everywhere else.

**`_ensure_schema` never commits internally, vs. committing schema creation
immediately wherever it runs.** An internal commit would make `_ensure_schema`
self-contained and easier to reason about in isolation, but committing a
caller-supplied `save(connection=...)` connection early is exactly the bug this
store exists to prevent: it would durably commit the caller's in-progress
transaction before their downstream write finishes, breaking the same-transaction
guarantee regardless of how correct the schema-creation logic itself is. Leaning on
`CREATE TABLE IF NOT EXISTS` being transactional in Postgres and letting each
call site's own commit boundary (the caller's, or `save()`/`load()`'s own) cover it
instead keeps exactly one commit rule in the entire store: only ever commit on a
connection this store opened or acquired itself, never one it was handed.

## Implementation

- `walbox/abc.py`: the async `CheckpointStore` Protocol, `CheckpointHandle`.
- `walbox/checkpoint.py`: `FileCheckpointStore`, `PostgresCheckpointStore` (including
  `from_pool`, `ConnectionPool`, and the `_acquire` indirection).
- `walbox/client.py`: constructs one `CheckpointHandle` per run and passes it as the
  second argument to every handler call (see Client Runtime, RFC 05, for the call
  site).
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
- `save(lsn, connection=conn)` called as a store's very first-ever operation, with no
  preceding `load()` or connection-less `save()`, still creates the backing table and
  succeeds -- the direct regression test for the ordering dependency the inline,
  per-call-site schema check removed.
- A store built via `from_pool` never calls `psycopg.AsyncConnection.connect`: every
  connection-less `load()`/`save()` comes from the pool instead, checked out and
  returned once per call; `save(lsn, connection=conn)` on a pool-backed store still
  bypasses the pool entirely, proving the same-transaction pattern is unaffected by
  which construction path built the store. Proven against both a hand-rolled fake
  pool (unit) and a real `psycopg_pool.AsyncConnectionPool` (integration).
- `CheckpointHandle.save`'s `_on_saved` hook receives a non-negative latency alongside
  the LSN, and a handler that never calls `save()` at all leaves the client's
  reported checkpoint latency at its unset default, rather than some stale prior
  value.
