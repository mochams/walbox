# RFC 03 — Transaction Assembly (including Streaming)

**Status:** Implemented
**Documented:** 2026-08-23

## Depends on

- ARCHITECTURE.md (error hierarchy — `ProtocolError` for message-sequencing
  violations; the correctness invariant: never expose an uncommitted or rolled-back
  transaction to the application).
- Wire Decoding (RFC 02) — consumes its `PgoutputMessage` value objects directly,
  including the `subxid` field on row-change messages.

Client Runtime (RFC 05) and Backpressure (RFC 06) depend on this feature's output
(`Transaction`), not the other way around.

## Summary / Context

**Problem.** pgoutput delivers a transaction's changes as a flat sequence of
messages on the wire — `Begin`, then some number of `Insert`/`Update`/`Delete`/
`Truncate`, then `Commit` — with no framing that groups them together as one unit.
An application should never see a change before it's known the whole transaction it
belongs to actually committed, and never see a rolled-back transaction's changes at
all. For a *large* transaction, PostgreSQL doesn't even wait for the commit before
sending changes: it streams them in chunks as they happen, interleaved with other
transactions' traffic, specifically so it never has to hold a huge changeset in
memory server-side. That means walbox has to buffer speculatively and be able to
discard precisely the right buffered work if a subtransaction (or the whole
transaction) is rolled back — without ever corrupting or reordering a transaction
that does commit.

**Business value.** This is the layer that turns "a stream of individual
row-change messages" into the one type applications actually see:
`Transaction(xid, commit_lsn, commit_time, changes)`, always complete, always
post-commit, always in commit order. Getting this right for both ordinary and
streamed transactions — including precise per-savepoint rollback discard — is what
lets walbox handle arbitrarily large transactions on published tables without ever
over- or under-delivering.

## Goals and Non-Goals

**Goals:**
- Emit a `Transaction` the instant its `Commit` (or `StreamCommit`) arrives — never
  before, never for a transaction that rolled back.
- Preserve commit order across multiple transactions, and change order within one
  transaction.
- Support PostgreSQL's streaming protocol: transactions whose changes arrive in
  chunks, before commit, interleaved with other (streamed or ordinary)
  transactions' traffic, without cross-contaminating buffers.
- Precisely discard only the changes a `ROLLBACK TO SAVEPOINT` inside a streamed
  transaction actually invalidates — not the whole transaction, and not changes
  made before or after the savepoint.
- Derive `Transaction.commit_lsn` correctly from the wire's commit fields, matching
  the convention every other LSN in walbox uses.

**Non-Goals:**
- No `streaming 'parallel'` (protocol version 4) support. Only serial streaming
  (`streaming 'on'`, protocol version 2) is negotiated; parallel streaming's extra
  optional fields on `StreamAbort` and the possibility of genuinely concurrent
  (nested) streaming brackets from multiple parallel apply workers are out of
  scope.
- No prepared-transaction support (protocol version 3) — unrelated feature, not
  built anywhere in walbox.
- No accounting of streamed-transaction memory against the bounded delivery queue's
  own memory bound (Backpressure, RFC 06). A large, or a large number of
  concurrent, streamed transactions can grow process memory independent of that
  configured bound — a known, deliberate v0.1 limitation (see README's
  Limitations).
- No change to the public `Transaction`/`ChangeEvent` shape based on whether a
  transaction was streamed server-side — that's invisible to the application, which
  receives the same `Transaction` either way, once, after commit.

## Proposed Design

### Ordinary (non-streamed) assembly

At most one transaction is open at a time in the non-streamed case, since
PostgreSQL never interleaves two non-streamed transactions' messages on one slot's
wire:

```python
def _begin(self, message: Begin) -> None: ...       # open a bucket for this xid
def _append_insert(self, message: Insert) -> None: ...  # append into the open bucket
def _finish(self, message: Commit) -> Transaction: ...  # close and emit
```

`Begin.final_lsn` is cross-checked against `Commit.commit_lsn` as a cheap, nearly-free
consistency assertion — both describe the same commit, and disagreement would
indicate a real protocol/decoder bug worth failing loudly on rather than silently
trusting either value.

### Generalizing to a keyed buffer for streaming

Streaming breaks the "one open transaction" assumption on purpose: while a large
transaction streams its chunks, other transactions — ordinary ones, and other
streamed ones — can be fully delivered in the gaps between chunks. The single-slot
state generalizes to:

```python
self._pending: dict[int, _Pending] = {}
self._current_xid: int | None = None        # the one open ordinary xid, if any
self._active_stream_xid: int | None = None  # the streaming bracket currently receiving, if any

@dataclass
class _Pending:
    changes: list[ChangeEvent] = field(default_factory=list)
    final_lsn: int | None = None       # None for a streamed bucket -- no Begin, no known final LSN yet
    subxact_boundaries: dict[int, int] = field(default_factory=dict)
```

A dict keyed by xid subsumes the single-slot case for free: a run with no streaming
simply never has more than one key in it at a time. `_begin()`/`_finish()` and
`stream_start(first_segment=True)`/`stream_commit()` are the same operation —
"open/close a bucket for this xid" — just reached via different message kinds. A bare
row-change message (which carries no top-level xid of its own, only a `subxid` while
streaming) resolves which bucket it belongs to as: the active streaming bracket, if
one is open, else the one open ordinary transaction, else a `ProtocolError`.

`StreamCommit` carries the same `commit_lsn`/`end_lsn`/`commit_time` fields as an
ordinary `Commit`, under a different tag and keyed explicitly by xid instead of "the
transaction `Begin` last opened" — so the same `_build_transaction` emission helper
used for ordinary commits is reused verbatim for streamed ones too, with no signature
change.

### `StreamAbort`: precise discard, both full-transaction and subtransaction

`StreamAbort` carries two xids: `xid` (the top-level transaction) and `subxid` (the
specific (sub)transaction being aborted; equal to `xid` for a full-transaction
abort).

Full-transaction discard (`subxid == xid`) is simple: pop and discard the whole
pending bucket. No special handling is needed for *ordinary* rolled-back
transactions at all — PostgreSQL's server-side reorder buffer never emits a
`Begin`/changes/`Commit` sequence for a transaction that never commits in the first
place; `StreamAbort` only exists because streamed changes are sent speculatively,
before PostgreSQL knows whether the transaction will commit.

Subtransaction discard (`subxid != xid`, i.e. `ROLLBACK TO SAVEPOINT`) is the harder,
precise case:

```python
def stream_abort(self, message: StreamAbort) -> None:
    if message.subxid == message.xid:
        self._pending.pop(message.xid, None)
        return
    pending = self._pending.get(message.xid)
    if pending is None:
        return  # abort for a top-level xid with no open bucket at all
    boundary = pending.subxact_boundaries.pop(message.subxid, None)
    if boundary is None:
        return  # this subxid never produced a change -- nothing to discard
    del pending.changes[boundary:]
    stale = [s for s, i in pending.subxact_boundaries.items() if i >= boundary]
    for s in stale:
        del pending.subxact_boundaries[s]
```

`subxact_boundaries` records, per subxid, the index in `changes` where that
subtransaction's changes first begin (recorded by `_append_change` the first time a
new subxid is seen). Truncating back to that boundary is correct because a
subtransaction's changes are always contiguous — nothing outside its scope can be
interleaved into the buffer while it's open, and a subxid, once closed, is never
reused. A nested savepoint needs no separate handling either: `ROLLBACK TO SAVEPOINT`
on an outer savepoint implicitly discards everything nested inside it, in one message,
and truncating from the outer savepoint's own boundary already removes the nested
one's changes too, since they're chronologically after it.

## Pros / Cons

**Single open-transaction slot generalized into a keyed dict, vs. two separate code
paths (one for streamed, one for ordinary).** A dict keyed by xid, with the ordinary
case simply never holding more than one entry, means one code path handles both —
no duplicated buffering/emission logic to keep in sync. The cost is a small amount of
extra indirection (`self._pending[xid]` instead of `self._current`) for every
non-streamed transaction too, even though it never needs the generality. Judged worth
it to avoid a parallel, duplicated implementation.

**Precise per-savepoint discard via boundary truncation, vs. logging a warning and
leaving the buffer untouched (over-inclusion).** An earlier version of this feature
treated `subxid != xid` as an unfixable protocol limitation, reasoning that the wire
gives no per-change indication of which subtransaction produced it — PostgreSQL's
protocol documentation describes the leading field on every streamed row-change
message simply as "Xid of the transaction," which reads as redundant with the
streaming bracket's own top-level xid. That reasoning turns out to be wrong: PostgreSQL's
own `pgoutput.c` sets that field to `change->txn->xid` — the *specific* (sub)transaction
that produced this particular change, with an explicit comment stating its purpose is
exactly so a subscriber can discard the right changes on abort. PostgreSQL's own
built-in apply worker (`worker.c`) does precisely this: recording each subxid's start
offset in a per-transaction spool file and truncating back to it on a subxid-scoped
abort. This feature adopts the identical technique in memory instead of a file. The
corrected version closes the gap entirely rather than settling for a documented
limitation — worth the extra state (`subxact_boundaries`) it costs to track.

**No memory accounting for streamed-transaction buffers against the bounded queue.**
Accounting for it would require either a shared memory budget across both buffers or
some form of backpressure specific to streaming, either of which is real added
complexity. Deferred as a stated v0.1 limitation rather than solved speculatively
before it's shown to matter in practice.

## Implementation

- `walbox/transaction.py` — `TransactionAssembler`, `_Pending`, `_begin`,
  `_append_insert`/`_append_update`/`_append_delete`/`_append_truncate`, `_finish`,
  `_build_transaction`, `stream_start`, `stream_stop`, `stream_commit`,
  `stream_abort`, `_active_xid`.
- `walbox/pgoutput.py` — `StreamStart`, `StreamStop`, `StreamCommit`, `StreamAbort`
  dataclasses and decode functions (see Wire Decoding, RFC 02, for their byte
  layouts).
- `walbox/transport.py` — `START_REPLICATION` negotiates `proto_version '2',
  streaming 'on'` to enable this feature at all; a transaction that never crosses
  `logical_decoding_work_mem` is completely unaffected either way.

## Testing

- A transaction's changes are only ever handed to the caller at `Commit`
  (`Insert`/`Update`/`Delete`/`Truncate`/`Begin` alone all return nothing); an
  out-of-order message (a second `Begin` before a `Commit`, a change with no open
  transaction, a `Commit` with no open transaction) raises `ProtocolError`.
- The derived `commit_lsn` is `commit.end_lsn - 1`, not `commit.commit_lsn` —
  verified with deliberately distinct values for the two fields so a wrong-field or
  swapped-field mistake can't pass unnoticed. This applies identically whether the
  commit arrived as an ordinary `Commit` or a `StreamCommit`, proving both paths get
  numerically identical checkpoint/feedback treatment.
- Two streamed transactions' chunks interleaved on the wire (`StreamStart(A) ...
  StreamStop, StreamStart(B) ... StreamStop, StreamStart(A) ... StreamStop,
  StreamCommit(A), StreamCommit(B)`) each yield a `Transaction` containing only
  their own changes, in their own order — no cross-contamination between buffers.
- An ordinary, fully-buffered transaction delivered in the middle of another xid's
  open streaming bracket is assembled correctly and independently of it.
- A full-transaction `StreamAbort` (`subxid == xid`) discards the whole buffer; a
  later stray message referencing that xid raises rather than resurrecting a stale
  buffer, and the xid can be legitimately reused by a subsequent transaction.
- A `ROLLBACK TO SAVEPOINT` inside a streamed transaction (`subxid != xid`) discards
  exactly the changes made under that savepoint, leaving changes made before and
  after it intact.
- A savepoint that produced no buffered changes before being rolled back is a
  no-op — there's no recorded boundary to discard from, so nothing else is
  disturbed.
- Rolling back an outer savepoint also discards a savepoint nested inside it, in one
  truncation, since PostgreSQL only ever sends one `StreamAbort` for the outer one,
  never a separate one for the nested savepoint.
- A subxid-scoped abort referencing a top-level xid with no open bucket at all
  (e.g. the bracket already closed via a prior full abort) is a no-op, not an error.
- Against a real, actively-streaming PostgreSQL connection: a transaction large
  enough to force real streaming, containing a real `SAVEPOINT`/`ROLLBACK TO
  SAVEPOINT`, delivers exactly the rows inserted before and after the savepoint —
  the doomed row is absent — proving the discard mechanism against the real wire
  protocol, not just hand-crafted messages. The equivalent whole-transaction
  `ROLLBACK` on a large streamed transaction delivers nothing at all, and a large
  streamed transaction held open in one session doesn't block a small ordinary
  transaction committing concurrently in another session from being delivered
  promptly.
