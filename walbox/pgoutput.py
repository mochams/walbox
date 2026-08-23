"""pgoutput logical replication message decoding.

Decodes the pgoutput sub-protocol carried as the opaque `payload` bytes
inside an `XLogData` message (`walbox/protocol.py`) -- this module never
imports from `protocol.py` and has no reason to see the outer replication
framing. Covers the message kinds needed for row-change replication:
`Relation`, `Begin`, `Insert`, `Update`, `Delete`, `Commit`, plus `Truncate`,
`Type`, and `Origin`, plus `StreamStart`, `StreamStop`, `StreamCommit`, and
`StreamAbort` for PostgreSQL's streamed (in-progress) transactions.
`Truncate` is actionable and decodes into `ChangeEvent`s just like
Insert/Update/Delete; `Type` and
`Origin` are decoded fully (so the byte stream never desyncs) but are not
actionable for the outbox pattern, so `walbox/client.py` logs and discards
them rather than forwarding them to `transaction.py`. Every other message
kind (prepared-transaction messages, the generic `Message` type, ...) is a
real gap the caller needs to know about, so it raises `DecodeError` rather
than being silently ignored.

Every non-null column value arrives in **text** format -- Postgres's own
output-function representation as a string (this is what requesting
`START_REPLICATION` without the separate `binary` suboption means on the
wire, independent of `proto_version`) -- so column values decode to
`str | None` exactly as sent, with no type coercion to native Python types.
"""

import logging
from collections.abc import Callable
from dataclasses import dataclass

from walbox.errors import DecodeError
from walbox.errors import ErrorContext

logger = logging.getLogger("walbox.pgoutput")

_BEGIN_LEN = 21  # Byte1 + Int64 final_lsn + Int64 commit_time + Int32 xid
_COMMIT_LEN = 26  # Byte1 + Int8 flags + 3 Int64 fields, see Commit's own docstring
_STREAM_START_LEN = 6  # Byte1 + Int32 xid + Int8 first_segment
_STREAM_STOP_LEN = 1  # Byte1 only
_STREAM_COMMIT_LEN = 30  # Byte1 + Int32 xid + Int8 (reserved) + 3 Int64 LSN/time fields
_STREAM_ABORT_LEN = 9  # Byte1 + Int32 xid + Int32 subxid


@dataclass(frozen=True, slots=True)
class Column:
    """One column's metadata from a `Relation` message."""

    name: str
    type_oid: int
    type_modifier: int
    is_key: bool


@dataclass(frozen=True, slots=True)
class Relation:
    """A table's schema, as of the most recent `Relation` message for it."""

    relation_id: int
    namespace: str
    name: str
    replica_identity: str  # 'd' | 'n' | 'f' | 'i'
    columns: tuple[Column, ...]

    @property
    def qualified_name(self) -> str:
        """The `namespace.name` form used to identify the table to callers."""
        return f"{self.namespace}.{self.name}"


@dataclass(frozen=True, slots=True)
class Begin:
    """The start of a transaction's stream of row-change messages."""

    final_lsn: int  # LSN of this transaction's eventual commit
    commit_time: int  # microseconds since the PostgreSQL epoch
    xid: int


@dataclass(frozen=True, slots=True)
class Insert:
    """A single inserted row, with its new values keyed by column name.

    `subxid` is the specific (sub)transaction this change belongs to,
    present only while a streaming bracket is open. It is PostgreSQL's own
    per-change transaction id (`pgoutput.c`'s `change->txn->xid`) -- *not*
    necessarily the streaming bracket's top-level xid. A change made
    directly in the top-level transaction carries that transaction's own
    id here; a change made under a `SAVEPOINT` carries that savepoint's own
    id instead. This is exactly what lets a subxid-scoped `StreamAbort`
    discard precisely the right changes later.
    """

    relation_id: int
    table: str  # resolved "namespace.name", filled in at decode time
    new: dict[str, str | None]
    subxid: int | None = None


@dataclass(frozen=True, slots=True)
class Update:
    """A single updated row.

    `old` is `None` when neither a `'K'` nor an `'O'` marker was present on
    the wire, which happens when REPLICA IDENTITY is `DEFAULT` (or `USING
    INDEX`) and none of the identity columns changed -- a legal, documented
    outcome, not a decode failure. When present, `old` holds only the
    columns the wire actually sent: key columns for a `'K'` marker, every
    column for `'O'`.

    `subxid` -- see `Insert.subxid`; identical meaning.
    """

    relation_id: int
    table: str  # resolved "namespace.name", filled in at decode time
    old: dict[str, str | None] | None
    new: dict[str, str | None]
    subxid: int | None = None


@dataclass(frozen=True, slots=True)
class Delete:
    """A single deleted row.

    `old` holds only the columns the wire actually sent: key columns for a
    `'K'` marker, every column for `'O'`. Unlike `Update`, the wire always
    sends one of these two markers for a `Delete`, so `old` is never `None`.

    `subxid` -- see `Insert.subxid`; identical meaning.
    """

    relation_id: int
    table: str  # resolved "namespace.name", filled in at decode time
    old: dict[str, str | None]
    subxid: int | None = None


@dataclass(frozen=True, slots=True)
class Commit:
    """The end of a transaction's stream of row-change messages."""

    flags: int  # currently always 0; decoded and kept for forward-compatibility
    commit_lsn: int  # LSN of the commit record itself
    end_lsn: int  # LSN immediately following the transaction
    commit_time: int


@dataclass(frozen=True, slots=True)
class Truncate:
    """A `TRUNCATE` of one or more published tables, resolved to table names.

    `cascade`/`restart_identity` are kept for logging even though they
    don't reach `ChangeEvent` -- `ChangeEvent` deliberately isn't extended
    with a field for them.

    `subxid` -- see `Insert.subxid`; identical meaning. `TRUNCATE` cannot be
    run inside a subtransaction that also does row-level DML on the same
    statement, but it can still occur after a `SAVEPOINT`, so this is
    tracked the same way as the row-change message kinds.
    """

    tables: tuple[str, ...]  # resolved "namespace.name", in wire order
    cascade: bool
    restart_identity: bool
    subxid: int | None = None


@dataclass(frozen=True, slots=True)
class Type:
    """A custom type's OID->name mapping, decoded but never acted on.

    Exists so a subscriber *could* map a column's type OID to a name for
    custom (non-builtin) types; walbox doesn't need that for the outbox
    pattern, so this is logged and discarded, never forwarded to
    `transaction.py`.
    """

    type_oid: int
    namespace: str
    name: str


@dataclass(frozen=True, slots=True)
class Origin:
    """The replication origin of the transaction, decoded but never acted on.

    Same treatment as `Type`: logged and discarded. Unlike every other
    pgoutput message kind, `Origin` never carries a leading `Xid` field,
    streamed or not -- but since walbox never acts on its content, it never
    needs to know which transaction it belongs to.
    """

    origin_lsn: int
    name: str


@dataclass(frozen=True, slots=True)
class StreamStart:
    """Marks the start of one chunk of a streamed (in-progress) transaction.

    `first_segment` is `True` for the first chunk ever sent for `xid`, and
    `False` for every later chunk of the same streamed transaction -- see
    `TransactionAssembler.stream_start` for how that distinguishes "open a
    new buffer" from "resume an existing one".
    """

    xid: int
    first_segment: bool


@dataclass(frozen=True, slots=True)
class StreamStop:
    """Marks the end of one chunk of a streamed transaction.

    Carries no fields of its own -- it is purely a chunk boundary. The
    streamed transaction's buffer stays open across it; only `StreamCommit`
    or `StreamAbort` closes it.
    """


@dataclass(frozen=True, slots=True)
class StreamCommit:
    """The commit of a streamed transaction.

    Carries the same `commit_lsn`/`end_lsn`/`commit_time` fields as an
    ordinary `Commit`, just keyed by `xid` instead of being scoped to
    "whatever transaction `Begin` last opened".
    """

    xid: int
    commit_lsn: int  # LSN of the commit record itself
    end_lsn: int  # LSN immediately following the transaction
    commit_time: int


@dataclass(frozen=True, slots=True)
class StreamAbort:
    """An abort of a streamed transaction or one of its subtransactions.

    `subxid == xid` for a full-transaction abort (the common case); a
    `subxid` differing from `xid` marks a `ROLLBACK TO SAVEPOINT` inside the
    streamed transaction -- see `TransactionAssembler.stream_abort` for how
    that precisely discards only the rolled-back savepoint's changes.
    """

    xid: int
    subxid: int


PgoutputMessage = (
    Relation
    | Begin
    | Insert
    | Update
    | Delete
    | Commit
    | Truncate
    | Type
    | Origin
    | StreamStart
    | StreamStop
    | StreamCommit
    | StreamAbort
)


class RelationCache:
    """Remembers every `Relation` message seen so far on a connection.

    Row-change messages reference a relation only by OID; resolving that
    OID to column names, order, and key-column flags requires remembering
    the most recent `Relation` message for it. A `Relation` message
    re-describing an OID (e.g. after a DDL change) simply overwrites the
    prior entry -- last write wins, with no separate invalidation logic
    needed.
    """

    def __init__(self) -> None:
        """Initialize an empty cache."""
        self._relations: dict[int, Relation] = {}

    def add(self, relation: Relation) -> None:
        """Register or replace the cached schema for `relation.relation_id`."""
        if relation.relation_id in self._relations:
            logger.debug(
                "overwriting cached relation %s",
                relation.qualified_name,
                extra={"relation": relation.qualified_name},
            )
        else:
            logger.debug(
                "caching relation %s",
                relation.qualified_name,
                extra={"relation": relation.qualified_name},
            )
        self._relations[relation.relation_id] = relation

    def get(self, relation_id: int) -> Relation:
        """Look up the cached schema for `relation_id`.

        Args:
            relation_id: The relation OID from a row-change message.

        Returns:
            The most recently cached `Relation` for that OID.

        Raises:
            DecodeError: If no `Relation` message has been seen yet for
                `relation_id`.
        """
        try:
            return self._relations[relation_id]
        except KeyError:
            message = f"no Relation message seen yet for relation {relation_id}"
            raise DecodeError(
                message,
                context=ErrorContext(message_type="Insert"),
            ) from None


def _read_cstring(buf: bytes, offset: int) -> tuple[str, int]:
    end = buf.index(b"\x00", offset)
    return buf[offset:end].decode("utf-8"), end + 1


def decode_relation(payload: bytes, *, streaming: bool = False) -> Relation:
    """Decode a `Relation` ('R') message.

    Args:
        payload: A complete pgoutput message payload.
        streaming: Whether a streaming bracket is currently
            open, meaning a leading `Int32 xid` field is present before the
            relation ID and must be skipped. Doesn't change the decoded
            value -- `Relation` never carries an xid -- only where the rest
            of the message starts.

    Returns:
        The decoded `Relation`, including its full column list.

    Raises:
        DecodeError: If `payload` has the wrong leading byte.
    """
    if payload[0:1] != b"R":
        message = "not a valid Relation message"
        raise DecodeError(message, context=ErrorContext(message_type="Relation"))
    offset = 1
    if streaming:
        offset += 4  # leading Xid, decoded but not retained on Relation
    relation_id = int.from_bytes(payload[offset : offset + 4], "big")
    offset += 4
    namespace, offset = _read_cstring(payload, offset)
    name, offset = _read_cstring(payload, offset)
    replica_identity = chr(payload[offset])
    offset += 1
    n_columns = int.from_bytes(payload[offset : offset + 2], "big")
    offset += 2
    columns = []
    for _ in range(n_columns):
        flags = payload[offset]
        offset += 1
        col_name, offset = _read_cstring(payload, offset)
        type_oid = int.from_bytes(payload[offset : offset + 4], "big")
        offset += 4
        type_modifier = int.from_bytes(payload[offset : offset + 4], "big", signed=True)
        offset += 4
        columns.append(
            Column(
                name=col_name,
                type_oid=type_oid,
                type_modifier=type_modifier,
                is_key=bool(flags & 1),
            ),
        )
    return Relation(
        relation_id=relation_id,
        namespace=namespace,
        name=name,
        replica_identity=replica_identity,
        columns=tuple(columns),
    )


def decode_begin(payload: bytes) -> Begin:
    """Decode a `Begin` ('B') message.

    Args:
        payload: A complete pgoutput message payload.

    Returns:
        The decoded `Begin`.

    Raises:
        DecodeError: If `payload` isn't exactly 21 bytes or has the wrong
            leading byte.
    """
    if len(payload) != _BEGIN_LEN or payload[0:1] != b"B":
        message = "not a valid Begin message"
        raise DecodeError(message, context=ErrorContext(message_type="Begin"))
    return Begin(
        final_lsn=int.from_bytes(payload[1:9], "big"),
        commit_time=int.from_bytes(payload[9:17], "big"),
        xid=int.from_bytes(payload[17:21], "big"),
    )


def decode_commit(payload: bytes) -> Commit:
    """Decode a `Commit` ('C') message.

    Args:
        payload: A complete pgoutput message payload.

    Returns:
        The decoded `Commit`, with both `commit_lsn` and `end_lsn` decoded
        faithfully -- choosing which one drives replication feedback is a
        policy decision for the checkpoint/feedback plans, not this decoder.

    Raises:
        DecodeError: If `payload` isn't exactly 26 bytes or has the wrong
            leading byte.
    """
    if len(payload) != _COMMIT_LEN or payload[0:1] != b"C":
        message = "not a valid Commit message"
        raise DecodeError(message, context=ErrorContext(message_type="Commit"))
    return Commit(
        flags=payload[1],
        commit_lsn=int.from_bytes(payload[2:10], "big"),
        end_lsn=int.from_bytes(payload[10:18], "big"),
        commit_time=int.from_bytes(payload[18:26], "big"),
    )


def _decode_tuple_data(
    buf: bytes,
    offset: int,
    columns: tuple[Column, ...],
) -> tuple[dict[str, str | None], int]:
    n_columns = int.from_bytes(buf[offset : offset + 2], "big")
    offset += 2
    if n_columns != len(columns):
        message = (
            f"TupleData has {n_columns} columns but the cached relation "
            f"has {len(columns)}"
        )
        raise DecodeError(message, context=ErrorContext(message_type="Insert"))
    row: dict[str, str | None] = {}
    for column in columns:
        marker = buf[offset : offset + 1]
        offset += 1
        if marker == b"n":
            row[column.name] = None
        elif marker == b"u":
            # Unchanged TOASTed value: Postgres omits it rather than resending
            # an unmodified large value. The column is left out of `row`
            # entirely, distinct from 'n' (an explicit SQL NULL) -- collapsing
            # the two would lose real data once a real value and an 'u' can
            # appear in the same row (Update/Delete old tuples).
            pass
        elif marker == b"t":
            length = int.from_bytes(buf[offset : offset + 4], "big")
            offset += 4
            row[column.name] = buf[offset : offset + length].decode("utf-8")
            offset += length
        else:
            message = f"unknown TupleData column marker {marker!r}"
            raise DecodeError(message, context=ErrorContext(message_type="Insert"))
    return row, offset


def decode_insert(
    payload: bytes,
    relations: RelationCache,
    *,
    streaming: bool = False,
) -> Insert:
    """Decode an `Insert` ('I') message.

    Args:
        payload: A complete pgoutput message payload.
        relations: The cache of previously seen `Relation` messages, used
            to resolve the message's relation OID to column names/order.
        streaming: Whether a streaming bracket is currently
            open, meaning a leading `Int32` subxact-id field is present and
            must be decoded -- see `Insert.subxid`.

    Returns:
        The decoded `Insert`, with `table` resolved to `namespace.name`,
        `new` keyed by real column names, and `subxid` set from the wire
        when `streaming` is true.

    Raises:
        DecodeError: If `payload` has the wrong leading byte, is missing
            the new-tuple marker, references a relation OID not yet seen
            in `relations`, or its `TupleData` column count doesn't match
            the cached relation's column count.
    """
    if payload[0:1] != b"I":
        message = "not a valid Insert message"
        raise DecodeError(message, context=ErrorContext(message_type="Insert"))
    offset = 1
    subxid: int | None = None
    if streaming:
        subxid = int.from_bytes(payload[offset : offset + 4], "big")
        offset += 4
    relation_id = int.from_bytes(payload[offset : offset + 4], "big")
    offset += 4
    if payload[offset : offset + 1] != b"N":
        message = "Insert is missing the new-tuple marker 'N'"
        raise DecodeError(message, context=ErrorContext(message_type="Insert"))
    offset += 1
    relation = relations.get(relation_id)
    new, _ = _decode_tuple_data(payload, offset, relation.columns)
    return Insert(
        relation_id=relation_id,
        table=relation.qualified_name,
        new=new,
        subxid=subxid,
    )


def _restrict_to_key_columns(
    row: dict[str, str | None],
    columns: tuple[Column, ...],
) -> dict[str, str | None]:
    """Drop non-key columns from a decoded `'K'`-marked tuple.

    PostgreSQL's key-only old tuple carries every column on the wire, not
    just the key ones -- but it marks every non-key column with the plain
    null marker `'n'`, indistinguishable on the wire from a real SQL NULL.
    `_decode_tuple_data` has no way to tell those apart, so the caller
    (which knows which columns are actually keys) must filter afterward.

    Returns:
        `row`, restricted to the columns `columns` marks as keys.
    """
    return {column.name: row[column.name] for column in columns if column.is_key}


def decode_update(
    payload: bytes,
    relations: RelationCache,
    *,
    streaming: bool = False,
) -> Update:
    """Decode an `Update` ('U') message.

    Args:
        payload: A complete pgoutput message payload.
        relations: The cache of previously seen `Relation` messages, used
            to resolve the message's relation OID to column names/order.
        streaming: Whether a streaming bracket is currently
            open, meaning a leading `Int32` subxact-id field is present and
            must be decoded -- see `Update.subxid`.

    Returns:
        The decoded `Update`, with `table` resolved to `namespace.name`,
        `new` keyed by real column names, `old` set to `None` when neither
        a `'K'` nor an `'O'` marker was present on the wire, and `subxid`
        set from the wire when `streaming` is true.

    Raises:
        DecodeError: If `payload` has the wrong leading byte, is missing
            the new-tuple marker, references a relation OID not yet seen
            in `relations`, or a `TupleData` column count doesn't match
            the cached relation's column count.
    """
    if payload[0:1] != b"U":
        message = "not a valid Update message"
        raise DecodeError(message, context=ErrorContext(message_type="Update"))
    offset = 1
    subxid: int | None = None
    if streaming:
        subxid = int.from_bytes(payload[offset : offset + 4], "big")
        offset += 4
    relation_id = int.from_bytes(payload[offset : offset + 4], "big")
    offset += 4
    relation = relations.get(relation_id)
    old: dict[str, str | None] | None = None
    marker = payload[offset : offset + 1]
    if marker in {b"K", b"O"}:
        offset += 1
        old, offset = _decode_tuple_data(payload, offset, relation.columns)
        if marker == b"K":
            old = _restrict_to_key_columns(old, relation.columns)
    if payload[offset : offset + 1] != b"N":
        message = "Update is missing the new-tuple marker 'N'"
        raise DecodeError(message, context=ErrorContext(message_type="Update"))
    offset += 1
    new, _ = _decode_tuple_data(payload, offset, relation.columns)
    return Update(
        relation_id=relation_id,
        table=relation.qualified_name,
        old=old,
        new=new,
        subxid=subxid,
    )


def decode_delete(
    payload: bytes,
    relations: RelationCache,
    *,
    streaming: bool = False,
) -> Delete:
    """Decode a `Delete` ('D') message.

    Args:
        payload: A complete pgoutput message payload.
        relations: The cache of previously seen `Relation` messages, used
            to resolve the message's relation OID to column names/order.
        streaming: Whether a streaming bracket is currently
            open, meaning a leading `Int32` subxact-id field is present and
            must be decoded -- see `Delete.subxid`.

    Returns:
        The decoded `Delete`, with `table` resolved to `namespace.name`,
        `old` keyed by real column names, and `subxid` set from the wire
        when `streaming` is true.

    Raises:
        DecodeError: If `payload` has the wrong leading byte, is missing
            both the key and old-row tuple markers, references a relation
            OID not yet seen in `relations`, or a `TupleData` column count
            doesn't match the cached relation's column count.
    """
    if payload[0:1] != b"D":
        message = "not a valid Delete message"
        raise DecodeError(message, context=ErrorContext(message_type="Delete"))
    offset = 1
    subxid: int | None = None
    if streaming:
        subxid = int.from_bytes(payload[offset : offset + 4], "big")
        offset += 4
    relation_id = int.from_bytes(payload[offset : offset + 4], "big")
    offset += 4
    relation = relations.get(relation_id)
    marker = payload[offset : offset + 1]
    offset += 1
    if marker not in {b"K", b"O"}:
        message = f"Delete is missing a key/old-row marker, got {marker!r}"
        raise DecodeError(message, context=ErrorContext(message_type="Delete"))
    old, _ = _decode_tuple_data(payload, offset, relation.columns)
    if marker == b"K":
        old = _restrict_to_key_columns(old, relation.columns)
    return Delete(
        relation_id=relation_id,
        table=relation.qualified_name,
        old=old,
        subxid=subxid,
    )


def decode_truncate(
    payload: bytes,
    relations: RelationCache,
    *,
    streaming: bool = False,
) -> Truncate:
    """Decode a `Truncate` ('T') message.

    Args:
        payload: A complete pgoutput message payload.
        relations: The cache of previously seen `Relation` messages, used
            to resolve each relation OID to a qualified table name.
        streaming: Whether a streaming bracket is currently
            open, meaning a leading `Int32` subxact-id field is present --
            once, before the relation count, not once per relation -- and
            must be decoded. See `Truncate.subxid`.

    Returns:
        The decoded `Truncate`, with `tables` resolved to `namespace.name`
        in the order the OIDs appeared on the wire, and `subxid` set from
        the wire when `streaming` is true.

    Raises:
        DecodeError: If `payload` has the wrong leading byte, or references
            a relation OID not yet seen in `relations`.
    """
    if payload[0:1] != b"T":
        message = "not a valid Truncate message"
        raise DecodeError(message, context=ErrorContext(message_type="Truncate"))
    offset = 1
    subxid: int | None = None
    if streaming:
        subxid = int.from_bytes(payload[offset : offset + 4], "big")
        offset += 4
    n_relations = int.from_bytes(payload[offset : offset + 4], "big")
    offset += 4
    flags = payload[offset]
    offset += 1
    tables = []
    for _ in range(n_relations):
        relation_id = int.from_bytes(payload[offset : offset + 4], "big")
        offset += 4
        tables.append(relations.get(relation_id).qualified_name)
    return Truncate(
        tables=tuple(tables),
        cascade=bool(flags & 1),
        restart_identity=bool(flags & 2),
        subxid=subxid,
    )


def decode_type(payload: bytes, *, streaming: bool = False) -> Type:
    """Decode a `Type` ('Y') message.

    Args:
        payload: A complete pgoutput message payload.
        streaming: Whether a streaming bracket is currently
            open, meaning a leading `Int32 xid` field is present before the
            type OID and must be skipped. Doesn't change the decoded value
            -- `Type` never carries an xid -- only where the rest of the
            message starts.

    Returns:
        The decoded `Type`.

    Raises:
        DecodeError: If `payload` has the wrong leading byte.
    """
    if payload[0:1] != b"Y":
        message = "not a valid Type message"
        raise DecodeError(message, context=ErrorContext(message_type="Type"))
    offset = 1
    if streaming:
        offset += 4  # leading Xid, decoded but not retained on Type
    type_oid = int.from_bytes(payload[offset : offset + 4], "big")
    offset += 4
    namespace, offset = _read_cstring(payload, offset)
    name, _ = _read_cstring(payload, offset)
    logger.debug(
        "decoded Type message oid=%s name=%s.%s",
        type_oid,
        namespace,
        name,
        extra={"message_type": "Type"},
    )
    return Type(type_oid=type_oid, namespace=namespace, name=name)


def decode_origin(payload: bytes) -> Origin:
    """Decode an `Origin` ('O') message.

    Args:
        payload: A complete pgoutput message payload.

    Returns:
        The decoded `Origin`.

    Raises:
        DecodeError: If `payload` has the wrong leading byte.
    """
    if payload[0:1] != b"O":
        message = "not a valid Origin message"
        raise DecodeError(message, context=ErrorContext(message_type="Origin"))
    origin_lsn = int.from_bytes(payload[1:9], "big")
    name, _ = _read_cstring(payload, 9)
    logger.debug(
        "decoded Origin message name=%s",
        name,
        extra={"message_type": "Origin", "lsn": origin_lsn},
    )
    return Origin(origin_lsn=origin_lsn, name=name)


def decode_stream_start(payload: bytes) -> StreamStart:
    """Decode a `StreamStart` ('S') message.

    Args:
        payload: A complete pgoutput message payload.

    Returns:
        The decoded `StreamStart`.

    Raises:
        DecodeError: If `payload` isn't exactly 6 bytes or has the wrong
            leading byte.
    """
    if len(payload) != _STREAM_START_LEN or payload[0:1] != b"S":
        message = "not a valid StreamStart message"
        raise DecodeError(message, context=ErrorContext(message_type="StreamStart"))
    xid = int.from_bytes(payload[1:5], "big")
    first_segment = bool(payload[5])
    return StreamStart(xid=xid, first_segment=first_segment)


def decode_stream_stop(payload: bytes) -> StreamStop:
    """Decode a `StreamStop` ('E') message.

    Args:
        payload: A complete pgoutput message payload.

    Returns:
        The decoded `StreamStop`.

    Raises:
        DecodeError: If `payload` isn't exactly 1 byte or has the wrong
            leading byte.
    """
    if len(payload) != _STREAM_STOP_LEN or payload[0:1] != b"E":
        message = "not a valid StreamStop message"
        raise DecodeError(message, context=ErrorContext(message_type="StreamStop"))
    return StreamStop()


def decode_stream_commit(payload: bytes) -> StreamCommit:
    """Decode a `StreamCommit` ('c') message.

    Args:
        payload: A complete pgoutput message payload.

    Returns:
        The decoded `StreamCommit`, carrying the same `commit_lsn`/
        `end_lsn`/`commit_time` fields as an ordinary `Commit`, keyed by
        `xid` instead of the implicit "whatever transaction `Begin` last
        opened".

    Raises:
        DecodeError: If `payload` isn't exactly 30 bytes or has the wrong
            leading byte.
    """
    if len(payload) != _STREAM_COMMIT_LEN or payload[0:1] != b"c":
        message = "not a valid StreamCommit message"
        raise DecodeError(message, context=ErrorContext(message_type="StreamCommit"))
    xid = int.from_bytes(payload[1:5], "big")
    # payload[5] is a reserved flags byte, always 0 -- skipped
    commit_lsn = int.from_bytes(payload[6:14], "big")
    end_lsn = int.from_bytes(payload[14:22], "big")
    commit_time = int.from_bytes(payload[22:30], "big")
    return StreamCommit(
        xid=xid,
        commit_lsn=commit_lsn,
        end_lsn=end_lsn,
        commit_time=commit_time,
    )


def decode_stream_abort(payload: bytes) -> StreamAbort:
    """Decode a `StreamAbort` ('A') message.

    Only the fixed 8-byte `(xid, subxid)` body is read -- `StreamAbort`'s
    two further optional fields (an LSN and a timestamp) only appear under
    `streaming 'parallel'` (protocol version 4), which is out of scope.

    Args:
        payload: A complete pgoutput message payload.

    Returns:
        The decoded `StreamAbort`.

    Raises:
        DecodeError: If `payload` isn't exactly 9 bytes or has the wrong
            leading byte.
    """
    if len(payload) != _STREAM_ABORT_LEN or payload[0:1] != b"A":
        message = "not a valid StreamAbort message"
        raise DecodeError(message, context=ErrorContext(message_type="StreamAbort"))
    xid = int.from_bytes(payload[1:5], "big")
    subxid = int.from_bytes(payload[5:9], "big")
    return StreamAbort(xid=xid, subxid=subxid)


_STATELESS_DECODERS: dict[bytes, Callable[[bytes], PgoutputMessage]] = {
    b"B": decode_begin,
    b"C": decode_commit,
    b"c": decode_stream_commit,
    b"A": decode_stream_abort,
    b"O": decode_origin,
}

_STREAMING_AWARE_RELATION_DECODERS: dict[
    bytes,
    Callable[..., PgoutputMessage],
] = {
    b"I": decode_insert,
    b"U": decode_update,
    b"D": decode_delete,
    b"T": decode_truncate,
}


class Decoder:
    """Stateful pgoutput decoder for one replication connection's lifetime.

    Tracks the relation cache (row-change messages only make sense in light
    of a previously-seen `Relation` message) and whether a streaming
    bracket for some xid is currently open --
    dispatching now depends on that flag (every Relation/Type/Insert/
    Update/Delete/Truncate message gains a leading `Int32 xid` field while
    a bracket is open), so it can no longer live in a free function that
    doesn't hold it.
    """

    def __init__(self) -> None:
        """Initialize with an empty relation cache and no streaming bracket open."""
        self._relations = RelationCache()
        self._streaming_active = False

    def decode(self, payload: bytes) -> PgoutputMessage:
        """Decode one pgoutput message payload, updating decoder state.

        A decoded `Relation` is both registered into the relation cache and
        returned, so a `Relation` message is never silently dropped -- the
        caller sees it like any other message and decides for itself that
        it's schema-only.

        Args:
            payload: A complete pgoutput message payload.

        Returns:
            The decoded message.

        Raises:
            DecodeError: If `payload`'s leading byte isn't one of the
                thirteen supported message kinds.
        """
        kind = payload[0:1]
        if kind == b"S":
            message = decode_stream_start(payload)
            self._streaming_active = True
            return message
        if kind == b"E":
            self._streaming_active = False
            return decode_stream_stop(payload)
        if kind == b"R":
            relation = decode_relation(payload, streaming=self._streaming_active)
            self._relations.add(relation)
            return relation
        if kind == b"Y":
            return decode_type(payload, streaming=self._streaming_active)
        if kind in _STREAMING_AWARE_RELATION_DECODERS:
            return _STREAMING_AWARE_RELATION_DECODERS[kind](
                payload,
                self._relations,
                streaming=self._streaming_active,
            )
        if kind in _STATELESS_DECODERS:
            return _STATELESS_DECODERS[kind](payload)
        message = f"unsupported pgoutput message type {kind!r}"
        raise DecodeError(
            message,
            context=ErrorContext(message_type=kind.decode(errors="replace")),
        )
