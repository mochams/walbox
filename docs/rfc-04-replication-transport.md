# RFC 04: Replication Transport

**Status:** Implemented
**Documented:** 2026-08-23

## Depends on

- ARCHITECTURE.md (error hierarchy: `ReplicationConnectionError`).
- [Replication Transport: Architecture Research](research/replication-transport-architecture.md):
  the accepted architecture this feature implements. Read that document for the
  primary-source evidence; this RFC applies its conclusions rather than
  re-litigating them.

Wire Decoding (RFC 02) and everything above it depend on this feature's output
(complete inner-message payloads), not the other way around. This feature never
imports `protocol.py` or `pgoutput.py`.

## Summary / Context

**Problem.** Psycopg 3 doesn't expose a convenient logical-replication connection
type the way Psycopg 2 did; walbox has to drive the underlying libpq connection and
the PostgreSQL replication protocol directly: open a replication-mode connection,
idempotently create the replication slot, issue `START_REPLICATION`, and then read
and write the COPY BOTH stream for the connection's entire lifetime. The obvious
approach (grab the raw socket file descriptor once COPY BOTH starts and read/write
it directly with asyncio's `sock_recv`/`sock_sendall`) has a fatal, easy-to-miss
flaw: whenever the connection negotiates TLS (the normal case for any managed/cloud
PostgreSQL: RDS, Cloud SQL, Supabase, Neon, etc.), the bytes sitting on that socket
are ciphertext, encrypted and decrypted by libpq transparently underneath its own I/O
functions. Reading raw bytes off the socket directly means reading raw ciphertext,
producing a bug that would pass every test against a local, no-TLS container and only
surface (silently, confusingly) against exactly the deployments a
production-oriented library most needs to support.

**Business value.** A transport that's correct *and* asyncio-native regardless of
whether TLS is in play is a prerequisite for walbox being usable against real,
managed PostgreSQL, which is most of where it would actually run in production. This
was validated with primary-source research (PostgreSQL's own reference client
source, PostgreSQL's official libpq documentation, and Psycopg 3's own source) before
being built, rather than assumed from a reference implementation that turned out to
have exactly this flaw.

## Goals and Non-Goals

**Goals:**
- Open a libpq replication connection, idempotently create the replication slot,
  issue `START_REPLICATION`, and drive the COPY BOTH stream for its entire lifetime,
  all through libpq's own functions, never a raw socket.
- Work identically, with no special-casing, whether or not the connection
  negotiates TLS.
- Use asyncio only to wait for the connection's file descriptor to become readable
  or writable, never to read or write connection data directly.
- Surface a small, four-method surface (`read`/`write`/`end_copy`/`close`, plus
  `connect`/`create_slot_if_missing`/`start_replication`) that returns one complete
  message per `read()` call, needing no client-side buffering.

**Non-Goals:**
- No pgoutput decoding, or interpretation of what's inside a payload beyond
  recognizing complete-message boundaries (which libpq already does). `read()`
  returns exactly what libpq's copy-data function returned; decoding it is Wire
  Decoding's job (RFC 02).
- No receiver/consumer task split, bounded queue, or keepalive-vs-backpressure race:
  those are Client Runtime (RFC 05) and Backpressure (RFC 06) concerns.
- No reconnect-with-backoff. `connect()` either succeeds or raises
  `ReplicationConnectionError`; retry policy is Client Runtime's (RFC 05).
- No publication creation. walbox creates its replication slot idempotently but
  never runs `CREATE PUBLICATION`; that stays a manual, one-time operational step
  (see README).
- No periodic `StandbyStatusUpdate`s on a timer: this feature exposes a raw send
  primitive; deciding when to call it with what bytes is feedback policy (Client
  Runtime, RFC 05).
- No resuming from a durable checkpoint: `start_replication` takes a plain
  `start_lsn: int`; wiring that to a `CheckpointStore` is Client Runtime's (RFC 05)
  and Checkpoint Store's (RFC 01) job.
- **Never constructs a `socket.socket` around the connection's own socket, and never
  calls `recv`/`send`/`sock_recv`/`sock_sendall` anywhere.** A hard architectural
  constraint, not an implementation detail left open.
- No TLS detection or special-casing of any kind: TLS, if negotiated, is handled
  entirely inside the libpq functions this feature already calls.

## Proposed Design

**One I/O regime for the whole connection.** Before and after `START_REPLICATION` is
accepted, the same mechanism drives everything: ask asyncio "is this fd ready?", then
call whichever libpq-backed method corresponds to what's being waited for. Two small
coroutines are the *only* thing asyncio ever does with the connection's file
descriptor:

```python
async def _wait_readable(self) -> None: ...   # loop.add_reader(pgconn.socket, ...)
async def _wait_writable(self) -> None: ...   # loop.add_writer(pgconn.socket, ...)
```

**Connecting** opens a libpq replication connection via Psycopg 3's
`AsyncConnection.connect`, with `replication="database"` merged into the DSN via
`psycopg.conninfo.make_conninfo` (correct regardless of whether the caller's DSN is a
keyword string or a `postgresql://` URI) and `autocommit=True` (replication protocol
commands don't tolerate an implicit transaction wrapper). If TLS is negotiated, it's
fully established here, before this feature ever calls another libpq method. Nothing
downstream needs to know whether that happened.

**Driving a command to completion** (slot creation, `START_REPLICATION`) follows the
standard non-blocking libpq pattern: `send_query`, then flush to completion
(`PQflush`, racing read- and write-readiness together per its own documented
deadlock-avoidance contract; a naive write-ready-only wait can deadlock, since the
server can block trying to send data and won't read the client's data until the
client reads its own), then drain results (`PQisBusy`/`PQconsumeInput`/`PQgetResult`)
until a terminal or COPY_BOTH status is reached. Draining stops the instant a
COPY_IN/COPY_OUT/COPY_BOTH result appears rather than looping to `None`
unconditionally; see Pros/Cons for why that distinction matters in practice, not
just in theory.

**Reading during COPY BOTH** is driven by libpq's non-blocking copy-data function,
following its own documented retry contract exactly: a `0` return means "not ready,
wait readable, consume input, retry"; a `-1` means the stream ended cleanly; any other
negative surfaces as a library exception, translated to
`ReplicationConnectionError`. Each `read()` call returns exactly one complete message
(an XLogData or keepalive frame, envelope already stripped by libpq), with no
partial reads and nothing for the caller to buffer.

**Writing during COPY BOTH, and ending it**, both retry on a "would block" return
after waiting writable, then flush to completion. `write()` sends one bare
already-encoded payload; `end_copy()` uses libpq's dedicated CopyDone function rather
than hand-assembling that message; libpq owns CopyData/CopyDone framing end to end,
never walbox.

**Closing** is a synchronous, immediate libpq teardown: no `await`, deliberately,
since `ReplicationClient.close()` (Client Runtime, RFC 05) must be callable directly
from a signal handler, which invokes plain callbacks, never coroutines. It doesn't
attempt to cleanly wind down an in-progress command, which is correct: inappropriate
mid-COPY, and unnecessary since the connection is being discarded regardless.

## Pros / Cons

The full three-way comparison (raw socket vs. libpq-driven vs. a custom ctypes/C
binding), including TLS-safety, fidelity to PostgreSQL's own reference client,
dependency footprint, socket-ownership semantics, and risk profile for each, is
worked out in detail in the
[research appendix](research/replication-transport-architecture.md). Summary:

**libpq-driven COPY BOTH (what shipped), vs. a raw socket after COPY BOTH starts
(the initial approach, rejected).** The raw-socket approach is simpler to reason
about at a glance and needs no new API surface beyond what a plain `socket.socket`
already offers, but it is confirmed broken under TLS, competes with libpq for
ownership of the same file descriptor, and needs its own hand-rolled CopyData framing
on both read and write. The libpq-driven approach needs no client-side framing at
all (the same functions that back Psycopg 3's own public `COPY` feature already
handle it), is TLS-safe by construction, and is function-for-function identical to
PostgreSQL's own bundled `pg_recvlogical` reference client. Its one real cost: it
relies on `psycopg.pq.PGconn` methods whose own reference documentation page is a
literal `TODO: finish documentation` stub: a genuine, if narrow, maintenance risk
(no docs-visible deprecation guarantee), mitigated by the fact that these methods
can't break without also breaking `COPY` for every Psycopg 3 user.

**A custom ctypes/C binding against raw `PGconn*`, considered and rejected outright.**
Would achieve the same TLS-safety and single-ownership properties as the libpq-driven
approach, but reimplements what Psycopg 3's existing `.pgconn` escape hatch already
provides for free, adding a new dependency-free-but-unaudited code surface for no
verified benefit.

**Draining `get_result()` to `None` unconditionally, vs. stopping at the first
COPY_* status.** The unconditional version is what a naive port of "drain all
results after a command" produces, and it's correct for ordinary commands. It is
actively wrong here: once `PQgetResult` returns a COPY_IN/COPY_OUT/COPY_BOTH result,
libpq's internal state stays in that COPY status rather than advancing, so a further
`get_result()` call doesn't wait on real I/O. It fabricates a new, empty copy-status
result out of thin air, forever, with zero further I/O. The unconditional version
CPU-spins the instant `START_REPLICATION` reaches COPY_BOTH and never returns. Fixed
by checking `result.status` inline and stopping at the first COPY_* status, the same
guard Psycopg 3's own internal generator code uses for the identical libpq behavior.

## Implementation

- `walbox/transport.py`: `ReplicationTransport` class (`connect`,
  `create_slot_if_missing`, `start_replication`, `read`, `write`, `end_copy`,
  `close`), LSN text encode/decode helpers (`_format_lsn`/`_parse_lsn`).
- `tests/integration/conftest.py`: the session-scoped `testcontainers` Postgres
  fixture (including a second, TLS-enabled container) and per-test table/publication
  fixtures.

## Testing

- LSN text formatting and parsing round-trip correctly, including the zero value and
  a value near the top of the 64-bit range.
- Retry/race behavior is exercised against a mocked connection (no real Postgres
  needed for these): a "would block" return from flush/write/copy-end retries
  correctly and cancels whatever readiness wait didn't win the race; a failure from
  any of these surfaces as `ReplicationConnectionError`, never a raw library
  exception.
- `START_REPLICATION`'s command text negotiates serial streaming
  (`proto_version '2'`, `streaming 'on'`), checked directly against bytes sent over
  a real socket pair, not just asserted on a mock.
- Against real PostgreSQL: connecting, idempotent slot creation (calling it twice is
  a no-op, and a non-duplicate failure still raises), and entering COPY BOTH all
  behave correctly; a row inserted after `START_REPLICATION` produces a decodable
  `XLogData`-shaped payload from `read()`.
- Terminating the backend mid-stream (simulating a dropped connection) makes the
  next `read()`/`end_copy()` call raise `ReplicationConnectionError` promptly,
  never hang or leak a raw library exception.
- `close()` is safe to call more than once.
- The direct regression test for the reason this feature's architecture exists:
  against a TLS-required container, the exact same connect/slot/replicate/read
  sequence works correctly over TLS. A raw-socket transport would hang or return
  garbage here; this one is unaffected by TLS being active at all. (A companion
  sanity check confirms the TLS container genuinely requires TLS, rather than merely
  offering it, so the positive test above is meaningful.)
