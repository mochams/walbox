# Replication Transport: Architecture Research

**Status:** Reference material — the research behind RFC 04 (Replication
Transport)'s architecture, kept for the primary-source evidence it gathered.

## Context

An earlier draft of RFC 04 proposed a transport design where Psycopg 3's
`AsyncConnection` is used only for connection setup and issuing `START_REPLICATION`,
after which its own libpq buffers are abandoned entirely: the underlying socket fd is
extracted via `int(pgconn.socket)`, rewrapped as a plain Python `socket.socket`, and
all subsequent COPY BOTH traffic is read/written with raw `loop.sock_recv`/
`sock_sendall`, with walbox manually parsing the CopyData wire envelope itself. This
mirrors the project's reference gist.

Before accepting that architecture, it was flagged as having a likely-fatal flaw:
PostgreSQL's SSL/TLS, when negotiated, is handled entirely inside libpq — once TLS is
active, the bytes actually sitting on the OS socket are ciphertext, encrypted/
decrypted by libpq (via OpenSSL) transparently underneath its own I/O functions. A
raw `recv()`/`send()` on the same fd, bypassing libpq, would read/write that
ciphertext directly and could never correctly speak the protocol whenever a
connection uses TLS — which is the normal case for any managed/cloud PostgreSQL (RDS,
Cloud SQL, Supabase, Neon, etc.), making this a correctness bug that would only
surface in exactly the deployments a "production-oriented" library most needs to
support.

This document verifies, from primary sources (PostgreSQL's own documentation and
source code, and Psycopg 3's own documentation and source code) rather than
assumption, whether libpq can instead remain the sole owner of the connection for the
entire replication session — including COPY BOTH — with asyncio used only to wait for
the connection's fd to become readable/writable, while every actual byte of I/O still
goes through libpq's own COPY functions. Three independent research passes were run
in parallel; their findings are synthesized below.

## Evidence gathered

**1. PostgreSQL's own reference client (`pg_recvlogical`) — read directly from
source**, `github.com/postgres/postgres`, `src/bin/pg_basebackup/pg_recvlogical.c`
(plus the sibling `receivelog.c` used by `pg_basebackup`/`pg_receivewal`, fetched for
structural comparison):

- The entire streaming loop (`StreamLogicalLog()`) is built exclusively on
  `PQgetCopyData(conn, &buf, 1)`, `PQconsumeInput(conn)`, `PQputCopyData(conn, buf,
  len)`, `PQflush(conn)`, and `select()`.
- `grep -n "recv(\|send(\|SSL"` across `pg_recvlogical.c`, `streamutil.c`, and
  `receivelog.c` returns **zero matches**. There is no raw socket I/O and no
  SSL-special-casing anywhere in PostgreSQL's own logical-replication client.
- `PQsocket(conn)` appears exactly twice, both purely as an argument to
  `FD_SET`/`select()` — never dereferenced, never wrapped in a socket object, never
  read from or written to directly.
- Exact feedback-sending code (`sendFeedback()`, lines 129–179): build the `'r'`
  StandbyStatusUpdate bytes in a stack buffer, then `PQputCopyData(conn, buf, len)`,
  then `PQflush(conn)`, checked together as one condition.
- Graceful shutdown sends `PQputCopyEnd(conn, NULL)` then `PQflush(conn)` — again,
  only COPY-API calls, never a raw socket close mid-protocol.
- `PQgetCopyData` already returns the payload with the outer CopyData (`'d'` +
  length) envelope already stripped off — the buffer handed back starts directly
  with `'w'` (XLogData) or `'k'` (keepalive).

This is about as strong a source as exists: it's PostgreSQL's own bundled, shipped
client for this exact protocol, and its I/O architecture is confirmed byte-for-byte
from the current source, not from documentation prose.

**2. PostgreSQL's official libpq documentation** — fetched and quoted directly from
`postgresql.org/docs/current/`:

- `PQgetCopyData`'s documented `async=1` contract, quoted verbatim: *"When async is
  true..., PQgetCopyData will not block...; it will return zero if the COPY is still
  in progress but no complete row is available. **In this case wait for read-ready
  and then call PQconsumeInput before calling PQgetCopyData again.**"* This is the
  exact retry protocol pg_recvlogical's C code implements.
- `PQflush`'s documented deadlock-avoidance contract, quoted verbatim: *"If it
  returns 1, wait for the socket to become read- or write-ready. If it becomes
  write-ready, call PQflush again. If it becomes read-ready, call PQconsumeInput,
  then call PQflush again... (It is necessary to check for read-ready... because the
  server can block trying to send us data... and won't read our data until we read
  its.)"* — a real, documented reason to race read-readiness and write-readiness
  together when flushing, not just wait on the write side.
- The single load-bearing TLS fact, from `protocol-flow.html` (a different page than
  initially expected, but authoritative): *"...continue with sending the usual
  StartupMessage. In this case **the StartupMessage and all subsequent data will be
  SSL-encrypted.**"* There is no carve-out for COPY/replication traffic — every byte
  after a successful TLS handshake, for the rest of the connection's life, is
  ciphertext at the socket level.
- One honest gap, reported rather than papered over by the research:
  `libpq-copy.html`'s prose for `PQgetCopyData`/`PQputCopyData` mentions only
  `COPY_OUT`/`COPY_IN`, never `COPY_BOTH`, by name — the applicability to `COPY_BOTH`
  is not spelled out in one authoritative sentence in the official docs. It is,
  however, conclusively established in practice by finding (1) above (PostgreSQL's
  own client using exactly these functions for exactly this mode) and by finding (3)
  below (these are the same functions backing Psycopg 3's own working COPY feature).
  Similarly, no single doc sentence says "PQsocket is for select()/poll() only, never
  raw I/O" — this is a strong, consistent convention across every documented usage
  example, not a written prohibition. These gaps don't weaken the conclusion; they
  mean the conclusion rests on convergent source-code + protocol evidence rather than
  one quotable warning.

**3. Psycopg 3's actual, current low-level API** — verified directly from source
(`github.com/psycopg/psycopg`, current `master`, cross-checked against the
installable `3.3.0`–`3.3.4` stable releases):

- `connection.pgconn` (a real `psycopg.pq.PGconn`) exposes, as genuinely working
  methods that call real libpq C functions via ctypes — confirmed by reading the
  actual implementation, not just a type stub: `get_copy_data(async_: int) ->
  tuple[int, memoryview]`, `put_copy_data(buffer) -> int`, `put_copy_end(error=None)
  -> int`, `consume_input() -> None`, `flush() -> int`, `is_busy() -> int`,
  `get_result() -> PGresult | None`, `socket` (property, the fd), `send_query(command:
  bytes) -> None`, `ssl_in_use` (property).
- Proof these aren't dead/experimental code: they are exactly what backs Psycopg 3's
  own public `Connection.copy()` feature. The call chain is traced directly from
  source: `Copy`/`AsyncCopy` → `generators.copy_to`/`copy_from`/`copy_end` →
  `pgconn.put_copy_data`/`get_copy_data`/`put_copy_end`/`flush`/`consume_input` → the
  real libpq C functions. Every `COPY` statement any Psycopg 3 user has ever run
  exercises this exact code path.
- `connection.pgconn` is officially documented (unlike the individual `PGconn`
  methods, whose reference page is a literal `TODO: finish documentation` stub) as
  the sanctioned low-level escape hatch: *"It can be used to send low level commands
  to PostgreSQL and access features not currently wrapped by Psycopg."*
- Psycopg 3 has its own internal generator/readiness-waiting pattern
  (`psycopg/generators.py`, `psycopg/waiting.py`) that drives every one of its own
  async operations: a generator yields `WAIT_R`/`WAIT_W`, and `waiting.wait_async`
  (for asyncio) resolves that with `loop.add_reader`/`add_writer`, structurally
  identical to what walbox would need to build for itself. This pattern is real and
  battle-tested (it's how *all* of Psycopg 3's async I/O works today) but is **not a
  public, documented API** — no `waiting.rst`/`generators.rst` page exists, and
  `psycopg/__init__.py` doesn't export either module. Depending on it directly would
  mean depending on genuinely undocumented internals with no compatibility
  guarantee, one level more fragile than the already-undocumented-but-stable
  `.pgconn` escape hatch.
- Directly relevant, previously unknown context: Psycopg 3's own maintainer has an
  open issue (`psycopg/psycopg#71`, since 2021) tracking real replication support,
  and has been actively iterating on it recently — a large proof-of-concept PR
  (`#1311`) was opened and then closed without merging, with the maintainer stating
  (most recent activity, ~3 months before this research) that he is "pursuing a
  different design direction." One small, real fix *has* already shipped in the
  current stable release (3.3.0+, PR `#1219`): psycopg3's own `cursor.execute()`/
  `conn.execute()` path no longer raises merely because a command (like
  `START_REPLICATION`) returns `COPY_BOTH` status — previously you'd have had to
  bypass `execute()` entirely just to issue the command without an exception. This
  doesn't add any new capability walbox needs (issuing the command via
  `pgconn.send_query()` directly, matching `pg_recvlogical`'s own approach,
  sidesteps the question entirely and needs no such fix), but it's useful context:
  official replication support may exist in Psycopg 3 someday, and isolating the
  low-level-API usage cleanly inside `transport.py` keeps a future migration cheap.
- The one existing real-world attempt at this problem — the project's own reference
  gist — was written *before* that fix landed, explicitly bypasses `pgconn`'s COPY
  methods, and its own comment claims this is to work around "psycopg's incomplete
  replication support." Per the trace above, that claim doesn't hold up: the
  low-level methods it avoided are real, working, and are exactly what Psycopg's own
  COPY feature depends on. The gist's raw-socket choice looks like it was made
  without discovering the low-level `.pgconn` COPY methods, not because they don't
  work.

## Answering the ten questions directly

**A. What is the correct libpq state machine for logical replication?**
Exactly what `pg_recvlogical` implements: issue replication commands via
`send_query`/`flush`/`is_busy`/`get_result` (simple query protocol — required for
`IDENTIFY_SYSTEM`/`CREATE_REPLICATION_SLOT`/`START_REPLICATION`, which are not
ordinary SQL); once the result status is `COPY_BOTH`, switch to a read loop of
`get_copy_data(async_=1)` → on `0`, wait-readable then `consume_input()` and retry;
and a write path of `put_copy_data(bytes)` → on `0`, wait-writable and retry →
`flush()` → on `1`, race wait-readable (then `consume_input()`) against
wait-writable, retry `flush()` until `0`.

**B. Can we implement it asynchronously without reading/writing the socket directly?**
Yes. Every function needed (`get_copy_data`, `put_copy_data`, `consume_input`,
`flush`, `is_busy`, `get_result`, `socket`, `send_query`) already exists and works on
Psycopg 3's `connection.pgconn`. Confirmed by direct source inspection, not
inference.

**C. Exactly how should asyncio interact with libpq?**
Only for readiness. Two small coroutines, `_wait_readable()`/`_wait_writable()`,
built on `loop.add_reader`/`add_writer` against `self._conn.pgconn.socket` — this is
precisely what RFC 04's earlier draft already built for its pre-COPY-BOTH phase
(`_wait_readable`/`_wait_writable`/`_flush`/`_drain_result`). The corrected
architecture's only real change is: **reuse those exact same helpers for the COPY
BOTH phase too**, instead of treating it as a second, fundamentally different regime.

**D. Exactly which Psycopg 3 APIs are sufficient?**
`connection.pgconn`'s `send_query`, `flush`, `is_busy`, `get_result`,
`get_copy_data`, `put_copy_data`, `put_copy_end`, `consume_input`, and `socket`. No
others.

**E. If Psycopg 3 is missing a required libpq function, what is the smallest safe
bridge?**
Moot — nothing is missing. No ctypes, no custom C/Cython extension, no dependency
beyond `psycopg` itself.

**F. How do we preserve TLS?**
By never touching the fd for anything except readiness. TLS is a byte-level
transformation libpq performs transparently inside
`consume_input`/`flush`/`get_copy_data`/`put_copy_data` (confirmed: `pg_recvlogical.c`
has zero SSL-aware code and works over TLS connections fine, because it never
bypasses these functions). As long as walbox only ever asks "is this fd ready?" and
then calls the corresponding `PQ*`-backed method, it is TLS-agnostic by construction
— it never needs to know or care whether TLS is active.

**G. How do we preserve libpq ownership of the connection?**
By never constructing a competing `socket.socket` around `pgconn.socket` at all. The
fd is read *only* via `loop.add_reader`/`add_writer` (which merely ask the OS "tell
me when this fd is ready," touching no connection state) and is otherwise used
exclusively as an opaque readiness token — every actual read or write of connection
data goes through a `pgconn` method, so libpq's internal buffering/state machine is
never bypassed or raced against.

**H. How do we handle COPY BOTH correctly?**
As one regime, not two: replace a raw-socket `read()`/`write()` with
`get_copy_data`/`put_copy_data`-driven versions using the same readiness helpers
already built for the pre-COPY-BOTH command phase. See the concrete API below.

**I. How should replication feedback be sent?**
`pgconn.put_copy_data(payload)` (retrying on a `0` return after waiting writable)
then `pgconn.flush()` (retrying per its documented read/write-readiness race until it
returns `0`). Note a real consequence for RFC 02: `put_copy_data` **itself**
constructs the outer CopyData envelope — the payload passed to it must be the bare
`'r' + ...` StandbyStatusUpdate bytes, not something already wrapped in a `'d' +
length` frame. A CopyData-wrapping step in `encode_standby_status_update` would be
unnecessary and wrong under this architecture (it would double-wrap the envelope).

**J. What should walbox's `ReplicationTransport` API look like?**

```python
class ReplicationTransport:
    def __init__(self, dsn: str, slot_name: str, publication_name: str) -> None:
        self._dsn = dsn
        self._slot_name = slot_name
        self._publication_name = publication_name
        self._conn: AsyncConnection[Any] | None = None

    async def connect(self) -> None:
        replication_dsn = make_conninfo(self._dsn, replication="database")
        self._conn = await AsyncConnection.connect(replication_dsn, autocommit=True)
        # TLS, if negotiated, is fully established here, inside libpq, before
        # this class ever touches pgconn directly for anything else.

    # -- readiness helpers: the ONLY thing asyncio does with the fd --
    async def _wait_readable(self) -> None: ...   # loop.add_reader(self._conn.pgconn.socket, ...)
    async def _wait_writable(self) -> None: ...   # loop.add_writer(self._conn.pgconn.socket, ...)

    # -- command phase: unchanged from the existing pre-COPY-BOTH draft --
    async def _flush(self) -> None: ...            # PQflush, racing read/write readiness per its documented contract
    async def _drain_result(self) -> PGresult: ...  # PQisBusy / PQconsumeInput / PQgetResult
    async def create_slot_if_missing(self) -> None: ...  # send_query + _flush + _drain_result, tolerating 42710
    async def start_replication(self, start_lsn: int) -> None: ...  # send_query + _flush + _drain_result, expect COPY_BOTH

    # -- COPY BOTH phase: now driven by pgconn, not a raw socket --
    async def read(self) -> bytes:
        """One complete replication message payload (CopyData envelope
        already stripped by libpq) -- an XLogData ('w') or keepalive ('k')
        frame. Never partial; no client-side buffering/framing needed."""
        pgconn = self._conn.pgconn
        while True:
            nbytes, data = pgconn.get_copy_data(1)
            if nbytes > 0:
                return bytes(data)
            if nbytes == -1:
                raise ReplicationConnectionError("replication stream ended", ...)
            if nbytes == -2:
                raise ReplicationConnectionError(f"error receiving copy data: {pgconn.get_error_message()}", ...)
            await self._wait_readable()   # nbytes == 0: PQgetCopyData's own documented retry contract
            pgconn.consume_input()

    async def write(self, payload: bytes) -> None:
        """Send one bare replication-protocol payload (e.g. a StandbyStatusUpdate
        'r' message) during COPY BOTH. Do not pre-wrap in a CopyData envelope --
        put_copy_data does that."""
        pgconn = self._conn.pgconn
        while pgconn.put_copy_data(payload) == 0:
            await self._wait_writable()
        await self._flush()

    def close(self) -> None:
        if self._conn is not None:
            self._conn.pgconn.finish()
            self._conn = None
```

Net effect on the public surface RFC 05 (Client Runtime) builds on: `read()` returns one complete
message per call (no partial frames, no client-side buffer/frame-splitting needed at
the transport-consumer boundary), and `write()` takes a bare payload instead of a
pre-wrapped frame. Both are *simpler* than a raw-socket version, not more complex —
the "two I/O regimes" framing goes away entirely; there is one regime
(asyncio-readiness-driven libpq calls) used for both the command phase and the COPY
BOTH phase.

## Explicit comparison of the three approaches

| | (1) Raw socket after COPY BOTH (rejected; the reference gist's approach) | (2) libpq `get_copy_data`/`put_copy_data` driven by asyncio readiness (recommended, and what shipped) | (3) Other (ctypes/ffi against raw `PGconn*`, or a custom C/Cython binding) |
|---|---|---|---|
| TLS-safe | **No.** Confirmed broken whenever the connection negotiates SSL — reads ciphertext directly, per `protocol-flow.html`'s "all subsequent data will be SSL-encrypted" and `pg_recvlogical`'s total absence of SSL-handling code (which only makes sense if the layer it uses handles TLS underneath it). | **Yes.** All I/O stays inside libpq's own functions, which handle TLS transparently — proven by PostgreSQL's own client working this way over TLS with zero SSL-specific code. | Yes, in principle, if implemented correctly — but reimplements what (2) already gets for free. |
| Matches PostgreSQL's own reference implementation | No — `pg_recvlogical`/`receivelog.c` never touch the raw socket. | **Yes**, exactly, function-for-function. | No direct precedent; would be a novel binding layer. |
| Needs anything beyond `psycopg` | No new dependency, but silently unsafe. | **No new dependency** — every required method already exists and works on `psycopg.pq.PGconn`. | Yes — ctypes boilerplate or a compiled extension, for no verified benefit over (2). |
| Socket ownership | Two independent owners of the same fd (libpq's internal state + a wrapped Python socket) — fragile even ignoring TLS. | Single owner (libpq) throughout; asyncio only ever asks "is this fd ready," never reads/writes it. | Single owner, same as (2), but via a hand-rolled binding instead of Psycopg's existing one. |
| Client-side complexity | Requires manual CopyData envelope parsing/framing on both read and write, plus buffering partial reads. | **Simpler** — `get_copy_data`/`put_copy_data` already handle framing; no client-side buffering needed. | Comparable to (2) once built, but with more code to write and maintain first. |
| Risk profile | Silent, TLS-conditional breakage — the kind of bug that passes every test against a local, no-TLS test container and fails only in production against a real (TLS-required) PostgreSQL instance. | Relies on real-but-officially-undocumented `PGconn` methods (docs page is a literal TODO stub) — a *maintenance* risk (could theoretically change without a docs-visible deprecation notice), not a *correctness* risk, and mitigated by these methods backing Psycopg's own public COPY feature (they can't break without breaking `COPY` for every Psycopg 3 user). | New, unaudited code (a ctypes/C bridge) is its own risk surface, for a problem (2) already solves. |

## Verdict

**Recommended transport architecture: (2) — drive libpq's own
`PQgetCopyData`/`PQputCopyData`/`PQconsumeInput`/`PQflush` (via Psycopg 3's
`connection.pgconn`) for the entire lifetime of the replication connection, including
COPY BOTH, using asyncio (`loop.add_reader`/`add_writer`) exclusively to wait for the
connection's file descriptor to become readable or writable. Never construct a
`socket.socket` around `pgconn.socket`, and never call
`sock_recv`/`sock_sendall`/raw `recv`/`send` on it.**

This is **TLS-safe** because every byte ever moves through a libpq function
(`consume_input`, `flush`, `get_copy_data`, `put_copy_data`) that already performs
whatever encryption/decryption the negotiated connection requires, transparently and
without walbox needing to know TLS is even involved — confirmed both by PostgreSQL's
own documented protocol behavior ("all subsequent data will be SSL-encrypted") and by
its own reference client (`pg_recvlogical`) containing zero SSL-aware code while
working correctly over TLS.

It is **async-safe** because the only thing asyncio ever does with the connection's
fd is ask the event loop "wake me when this is ready" — exactly the same
non-blocking-readiness pattern Psycopg 3 already uses internally for all of its own
async operations (its private `generators`/`waiting` modules), and exactly what RFC
05's pre-COPY-BOTH command phase already correctly built; this recommendation only
extends that same, already-designed pattern to also cover COPY BOTH, rather than
introducing a second, different I/O mechanism partway through the connection's life.

It is **libpq-correct** because it is, function-for-function, the identical state
machine PostgreSQL's own bundled `pg_recvlogical` client uses for this exact
protocol — verified directly from its current source, not inferred from
documentation prose — and because every method it depends on (`get_copy_data`,
`put_copy_data`, `consume_input`, `flush`, `is_busy`, `get_result`, `socket`) is
confirmed, by tracing Psycopg 3's own source, to be real, working code that already
backs Psycopg 3's own public `COPY` feature — not a gap that needs a ctypes bridge or
custom binding to fill.

## Follow-up: what happened next

This document was research only — it did not itself modify any implementation. Its
conclusion was accepted, and three areas were revised accordingly, all reflected in
their own current RFCs rather than restated here:

- **RFC 04** — `connect()`'s raw-socket setup and the raw-socket `read()`/`write()`
  were replaced with the `pgconn`-driven versions shown under question J above.
  `_wait_readable`/`_wait_writable`/`_flush`/`_drain_result`/`create_slot_if_missing`/
  `start_replication` were unaffected — they already worked this way.
- **RFC 02** — manual CopyData envelope framing was dropped from `protocol.py`
  entirely (libpq handles it in both directions now); `encode_standby_status_update`
  returns the bare `'r' + ...` payload instead of a pre-wrapped CopyData frame.
- **RFC 05 (Client Runtime)** — the receive loop's client-side buffer/frame-splitting logic was
  dropped, since `transport.read()` yields one complete replication message per call
  rather than possibly-partial raw bytes.
