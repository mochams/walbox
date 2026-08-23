# RFC 02: Wire Decoding (Protocol Framing and pgoutput)

**Status:** Implemented
**Documented:** 2026-08-23

## Depends on

- ARCHITECTURE.md (error hierarchy: `ProtocolError` for sequencing/framing
  violations, `DecodeError` for malformed message bytes).
- Replication Transport (RFC 04): supplies the complete, already-unwrapped byte
  payloads this feature decodes; this feature never touches a socket or a
  connection.

Transaction Assembly (RFC 03) depends on this feature's output (`PgoutputMessage`
value objects), not the other way around.

## Summary / Context

**Problem.** PostgreSQL's logical replication stream is not self-describing bytes.
It's two nested wire protocols. The outer layer (`XLogData`, `PrimaryKeepaliveMessage`,
`StandbyStatusUpdate`) rides inside COPY BOTH and tells the client "here is a chunk of
WAL data" or "the server wants to know you're alive." The inner layer (pgoutput:
`Relation`, `Begin`, `Insert`, `Update`, `Delete`, `Truncate`, `Commit`, `Type`,
`Origin`, and the four streaming message kinds) is the actual logical description of
what changed in the database, carried as opaque bytes inside every `XLogData`. Without
a correct, tested decoder for both layers, nothing downstream (transaction assembly,
checkpointing, the application's own handler) has anything meaningful to work with,
and a subtly wrong byte offset produces silent data corruption rather than a loud
failure.

**Business value.** This is the layer that turns "PostgreSQL's internal WAL
representation" into "structured, typed Python values an application can reason
about": every `ChangeEvent`'s `new`/`old` dict, every `Transaction`'s LSNs, ultimately
trace back to being decoded correctly here. Getting the REPLICA IDENTITY variants and
the streaming-mode wire shapes right (not just the common insert-only, non-streamed
case) is what makes walbox usable as a *general* logical-replication runtime rather
than one narrowly special-cased for insert-only outbox tables.

## Goals and Non-Goals

**Goals:**
- Pure decode/encode functions for the outer replication-protocol messages
  (`protocol.py`): `XLogData`, `PrimaryKeepaliveMessage`, `StandbyStatusUpdate`.
- A pure, stateful-only-where-necessary decoder for the inner pgoutput sub-protocol
  (`pgoutput.py`): every message kind needed for row-change replication, including
  all three REPLICA IDENTITY wire shapes for Update/Delete, and Truncate/Type/Origin.
- Correct, verified LSN "+1" semantics on the one message walbox itself constructs
  (`StandbyStatusUpdate`).
- No silent message-type drops: an unrecognized or not-yet-understood message kind
  raises rather than being quietly skipped.

**Non-Goals:**
- No CopyData envelope handling, inbound or outbound, anywhere in this feature.
  Libpq already strips/constructs that layer (Replication Transport, RFC 04); this
  feature only ever sees or produces a complete, bare payload.
- No `CopyBothResponse` parsing: that message arrives through libpq's ordinary
  result machinery, never as a payload this feature decodes.
- No type coercion from Postgres column types to native Python types. Every non-null
  column value decodes to `str | None` exactly as sent (`proto_version '1'`'s text
  format); converting `"123"` to `int(123)` would be a deliberate feature to layer
  on top, not something bolted on speculatively into the decoder.
- No decision about *which* of `Commit`'s two LSN fields (`commit_lsn` vs.
  `end_lsn`) drives replication feedback or checkpointing, and no decision about
  *when*/with *what* LSNs to send a `StandbyStatusUpdate`. Both are decoded/encoded
  faithfully; the policy decisions belong to Transaction Assembly (RFC 03) and Client
  Runtime (RFC 05).
- No relation-cache invalidation *logic*: a `Relation` message re-describing an
  already-known OID (e.g. after `ALTER TABLE ... ADD COLUMN`) simply overwrites the
  cached entry, which is already the correct behavior with no extra code needed.
- No surfacing of Type/Origin content, or of Truncate's `CASCADE`/`RESTART IDENTITY`
  flags, to the application. They're decoded fully (so the byte stream never
  desyncs) but not forwarded; see the Client Runtime RFC for where Type/Origin get
  filtered before they'd otherwise reach transaction assembly.
- The pgoutput `Message` type (`'M'`, from `pg_logical_emit_message()`) and
  prepared-transaction messages (protocol version 3) are not decoded anywhere in
  walbox: a stated, explicit gap, and encountering one raises `DecodeError` rather than
  being silently skipped.

## Proposed Design

### Outer messages (`protocol.py`)

```python
@dataclass(frozen=True, slots=True)
class XLogData:
    wal_start: int   # LSN of the first byte of `payload`
    wal_end: int     # server's reported current end-of-WAL at send time
    send_time: int
    payload: bytes   # opaque pgoutput bytes, decoded by pgoutput.py

@dataclass(frozen=True, slots=True)
class PrimaryKeepalive:
    wal_end: int
    send_time: int
    reply_requested: bool

@dataclass(frozen=True, slots=True)
class StandbyStatusUpdate:
    written_lsn: int
    flushed_lsn: int
    applied_lsn: int    # walbox has no separate "apply" stage; callers pass flushed_lsn here too
    client_time: int
    reply_requested: bool
```

Decoding dispatches on the leading byte over a complete payload: no
length/remainder bookkeeping needed, since the transport already guarantees one
complete message per call. 25 bytes minimum for `XLogData` (1 type byte + three
`Int64` fields before any payload); 18 bytes exactly for `PrimaryKeepaliveMessage`
(1 + 8 + 8 + 1, no payload at all).

**The `+1` rule.** PostgreSQL's own documentation describes each `StandbyStatusUpdate`
LSN field as "the location of the last WAL byte + 1": the byte position *after* the
last byte actually processed, not the position of that byte itself. Getting this
wrong is a real correctness hazard: combined with `START_REPLICATION` resuming from
`max(requested_lsn, slot's confirmed_flush_lsn)`, an inconsistent `+1` could, on a
subsequent restart, make PostgreSQL believe more has been durably processed than
truly has. Every other LSN value anywhere in walbox is stored raw and un-adjusted.
The `+1` is applied in exactly one place, at the moment `encode_standby_status_update`
builds the wire bytes, so it can only ever be wrong in one spot if it's wrong at all.

The encoded result is a **bare** 34-byte payload (`b"r" + 3×Int64 + Int64 + Byte1`).
It is handed directly to the transport's `write()`, which passes it to libpq's own
`put_copy_data()` to be wrapped in a CopyData envelope. `protocol.py` must not wrap
it itself, or the message would be double-framed.

### Inner messages (`pgoutput.py`)

The relation cache is the one piece of real, necessary state in an otherwise-pure
module. Row-change messages reference a relation only by OID, and resolving that to
column names/order/key-flags requires remembering the most recent `Relation` message
for it:

```python
class RelationCache:
    def add(self, relation: Relation) -> None: ...
    def get(self, relation_id: int) -> Relation: ...  # DecodeError if never seen
```

`Insert`/`Update`/`Delete`/`Truncate` all resolve their relation to a fully-qualified
table name (`namespace.name`) at decode time, so nothing downstream ever needs to
know relations or OIDs exist at all. `transaction.py` just reads `message.table` off
an already-resolved value object.

**Row-change wire shapes**, decoded with a running byte offset (no `Reader`
abstraction; every function takes `payload: bytes` and manages a local `offset`
integer, matching the wire's own linear structure):

```
Insert:  Byte1('I') Int32(relation_id) Byte1('N') TupleData
Update:  Byte1('U') Int32(relation_id)
         [ Byte1('K') TupleData ]   -- old tuple is key-columns-only
         [ Byte1('O') TupleData ]   -- old tuple is the full old row
         Byte1('N') TupleData      -- new tuple (always present)
Delete:  Byte1('D') Int32(relation_id)
         Byte1('K') TupleData | Byte1('O') TupleData
Truncate: Byte1('T') Int32(n_relations) Int8(flags) Int32(relation_id) × n_relations
```

`TupleData` is `Int16` column count, then per column one of `Byte1('n')` (SQL NULL),
`Byte1('u')` (unchanged TOASTed value, omitted from the result dict entirely; distinct
from an explicit NULL), or `Byte1('t')` (text value: `Int32` length + UTF-8 bytes).

Exactly one of `'K'`/`'O'`/neither appears for Update; PostgreSQL's publication
machinery normally guarantees one of `'K'`/`'O'` is always present for Delete, but the
decoder doesn't assume it: a missing marker on Delete raises `DecodeError` rather
than silently treating `old` as `None`, since (unlike Update) Delete's grammar has no
legal no-marker case.

**Truncate fans out to one `ChangeEvent` per table.** A single `TRUNCATE a, b, c`
produces one wire message covering three OIDs; rather than inventing a multi-table
`ChangeEvent` shape, it becomes one `ChangeEvent(kind=ChangeKind.TRUNCATE, ...)` per
table, in wire order, keeping `ChangeEvent` uniform (one event, one table, always).
`ChangeEvent.kind` is a `StrEnum` (`ChangeKind`), not a bare `str`: its members compare
equal to and serialize as their plain string values, so existing string comparisons
keep working unchanged.

**Type and Origin** are decoded fully (so the byte stream never desyncs) but carry no
actionable content for the outbox pattern. See the Client Runtime RFC for where
they're filtered before reaching transaction assembly.

### Streaming's effect on wire shapes

While a streaming bracket for some xid is open (Transaction Assembly, RFC 03), every
`Relation`/`Type`/`Insert`/`Update`/`Delete`/`Truncate` message gains a leading
`Int32` xid field that's **absent** in non-streamed messages of the same kind. Each
affected decode function gains a `streaming: bool = False` keyword parameter and one
small conditional read at the top; the *decoding* logic for the rest of the message
is unchanged. For `Insert`/`Update`/`Delete`/`Truncate`, that leading value is kept as
a `subxid: int | None` field on the resulting value object (PostgreSQL's own
per-change transaction id, not necessarily the top-level bracket's own xid); for
`Relation`/`Type` it's decoded and discarded (their own decoded value never changes,
only where the rest of the message starts). `Origin`'s wire shape never carries an
xid at all, streamed or not. Why `subxid` exists and what it's used for is Transaction
Assembly's (RFC 03) story in full. This feature's job stops at decoding it
correctly.

## Pros / Cons

**Raw `bytes` + running `offset`, vs. a `Reader`/`BinaryIO` abstraction.** A `Reader`
class would remove some repetition across decode functions, but every decode function
here already has an obvious, linear, single-pass structure that matches the wire
format itself byte-for-byte. An abstraction over that would add a layer of
indirection for a problem (repeated offset bookkeeping) that's small and localized.
Kept things at the level of "read exactly what the protocol spec says, in the order
it says it."

**Two decode layers (`protocol.py`, `pgoutput.py`), never merged.** Keeping the outer
replication-protocol layer and the inner pgoutput sub-protocol as separate modules
with no dependency between them (neither imports the other) means `pgoutput.py` is
fully testable against synthetic payloads with zero knowledge of COPY BOTH, XLogData,
or keepalives ever existing, and vice versa. The cost is that a caller (Client
Runtime, RFC 05) has to explicitly plumb `XLogData.payload` into the pgoutput decoder
itself; judged a small, one-line cost for a real testability and separation-of-concerns
win.

**`'u'` (unchanged TOAST) omits the column, vs. collapsing it to `None`.** An earlier
version of the tuple decoder collapsed an `'u'`-marked column to `None`, on the theory
that `Insert`'s mandatory tuple never actually emits `'u'` in practice (a freshly
inserted row has no prior value to leave unchanged), so the distinction seemed
academic. That reasoning breaks the moment Update's old/new tuples reuse the same
function: a real value and an `'u'` can appear in the same row there, and
conflating "unchanged" with "actually NULL" would lose real data. Fixed to omit the
key from the result dict entirely, distinguishing it cleanly from an explicit SQL
NULL.

**`_restrict_to_key_columns` as a caller-side filter, vs. relying on the wire to only
send key columns.** The original assumption was that a `'K'` (key-only) tuple's
non-key columns simply wouldn't be present on the wire at all. Verified against a real
PostgreSQL server, that's wrong: `pgoutput.c` marks every non-key column `'n'` (the
plain SQL-NULL marker) in a `'K'` tuple, byte-for-byte indistinguishable from a
genuine NULL. The generic tuple decoder has no way to tell "not part of the key" from
"really NULL" on its own, so a small helper filters the already-decoded row down to
`is_key=True` columns afterward, using schema knowledge the generic decoder
deliberately doesn't have. This was a real correction found by testing against
PostgreSQL directly, not a design choice made up front.

## Implementation

- `walbox/protocol.py`: `XLogData`, `PrimaryKeepalive`, `StandbyStatusUpdate`
  dataclasses; `decode_xlog_data`, `decode_primary_keepalive`,
  `decode_replication_message`, `encode_standby_status_update`; `pg_now_micros`.
- `walbox/pgoutput.py`: `Column`, `Relation`, `Begin`, `Insert`, `Update`, `Delete`,
  `Commit`, `Truncate`, `Type`, `Origin` dataclasses; `RelationCache`; the
  `decode_*` functions for each; the `Decoder` wrapper class bundling the cache with
  dispatch.

## Testing

- Each outer/inner message's fields decode to distinct, correctly-positioned values:
  every test that has more than one same-typed field (e.g. `commit_lsn`/`end_lsn`,
  or `wal_start`/`wal_end`) uses deliberately distinct values so a field-order swap
  can't pass unnoticed.
- Malformed input (wrong leading byte, wrong length, an unrecognized TupleData
  marker, a relation OID never seen in a prior `Relation` message) raises
  `DecodeError` rather than producing a partially-wrong value or silently
  continuing.
- `encode_standby_status_update` applies `+1` to all three LSN fields and returns a
  bare payload (no CopyData envelope), verified by parsing the raw bytes back out
  directly, not through a round-trip decoder (there isn't one; this message only
  ever travels client→server).
- REPLICA IDENTITY `DEFAULT` (no old tuple, key changed), `DEFAULT` (old tuple present
  when the key itself changes), and `FULL` (complete old row) all decode Update/Delete
  correctly, with a `'K'` tuple's non-key columns correctly filtered out rather than
  appearing as spurious NULLs.
- A `TRUNCATE` naming several tables fans out into one `Truncate` value with all
  table names resolved in wire order; `CASCADE`/`RESTART IDENTITY` flag combinations
  decode correctly even though they're never surfaced further.
- A `Type` or `Origin` message decodes fully and doesn't desync the byte stream for
  whatever message follows it, even though its content is never used.
- A second `Relation` message for an already-cached OID (simulating a live schema
  change) overwrites the cached entry, and a subsequent row-change message for that
  OID decodes using the *new* column list.
- Streaming-mode decoding: with `streaming=True`, each affected decode function
  correctly skips (or, for row-change kinds, retains as `subxid`) the extra leading
  xid field that only appears while a streaming bracket is open; with the default
  `streaming=False`, that field is absent and decoding is unaffected.
