"""Transaction assembly: pgoutput messages -> `abc.Transaction`.

Buffers each open transaction's changes, keyed by xid, and emits an
`abc.Transaction` as soon as its `Commit`/`StreamCommit` arrives. A
non-streamed slot never has more than one transaction open at a time, but
Postgres can interleave a streamed transaction's chunks with others to
avoid buffering a huge transaction whole server-side. `self._pending`
tracks every open xid in a `dict` to handle both cases uniformly.
"""

import logging
from dataclasses import dataclass
from dataclasses import field

from walbox.abc import ChangeEvent
from walbox.abc import ChangeKind
from walbox.abc import Transaction
from walbox.errors import ErrorContext
from walbox.errors import ProtocolError
from walbox.pgoutput import Begin
from walbox.pgoutput import Commit
from walbox.pgoutput import Delete
from walbox.pgoutput import Insert
from walbox.pgoutput import PgoutputMessage
from walbox.pgoutput import Relation
from walbox.pgoutput import StreamAbort
from walbox.pgoutput import StreamCommit
from walbox.pgoutput import StreamStart
from walbox.pgoutput import StreamStop
from walbox.pgoutput import Truncate
from walbox.pgoutput import Update

logger = logging.getLogger("walbox.transaction")


@dataclass
class _Pending:
    """Mutable accumulator for one xid's changes while its transaction is open.

    `final_lsn` comes from `Begin.final_lsn` and is cross-checked against
    `Commit.commit_lsn` in `_finish`. It's `None` for a streamed
    transaction's bucket, since Postgres doesn't know a streamed
    transaction's final LSN until it commits.

    `subxact_boundaries` maps a subtransaction id to the index in `changes`
    where that subtransaction's changes begin. Because a subtransaction's
    changes are always contiguous, truncating `changes` back to that index
    on a subxid-scoped `StreamAbort` discards exactly the right ones. Empty
    for a non-streamed transaction, since only streamed row-change messages
    carry a subxid.
    """

    changes: list[ChangeEvent] = field(default_factory=list)
    final_lsn: int | None = None
    subxact_boundaries: dict[int, int] = field(default_factory=dict)


class TransactionAssembler:
    """Assembles decoded pgoutput messages into complete `Transaction`s.

    Call `feed` once per decoded message. It returns the assembled
    `Transaction` when a `Commit` or `StreamCommit` completes it, and `None`
    otherwise. Multiple xids can be buffered at once (`self._pending`), but
    at most one ordinary transaction and one actively-streaming bracket can
    be open at a time; `self._current_xid` and `self._active_stream_xid`
    track which pending bucket a bare row-change message belongs to.
    """

    def __init__(self) -> None:
        """Initialize with no open or pending transactions."""
        self._pending: dict[int, _Pending] = {}
        self._current_xid: int | None = None
        self._active_stream_xid: int | None = None

    def feed(self, message: PgoutputMessage) -> Transaction | None:
        """Consume one decoded pgoutput message, in wire order.

        `client.py` filters out `Type`/`Origin`/`Message` before calling this.

        Returns:
            The assembled `Transaction` when a `Commit` or `StreamCommit`
            completes it, otherwise `None`.

        Raises:
            AssertionError: If `message` is a variant not handled above.
        """
        match message:
            case Begin():
                self._begin(message)
            case Insert():
                self._append_insert(message)
            case Update():
                self._append_update(message)
            case Delete():
                self._append_delete(message)
            case Truncate():
                self._append_truncate(message)
            case Commit():
                return self._finish(message)
            case Relation():
                pass  # schema-only; already applied by pgoutput.Decoder
            case StreamStart() | StreamStop() | StreamCommit() | StreamAbort():
                return self._feed_stream_message(message)
            case _:  # pragma: no cover, Type/Origin/Message are filtered out
                # by client.py before reaching feed(); anything else is a bug.
                unreachable = f"unhandled pgoutput message type {message!r}"
                raise AssertionError(unreachable)
        return None

    def _feed_stream_message(
        self,
        message: StreamStart | StreamStop | StreamCommit | StreamAbort,
    ) -> Transaction | None:
        """Dispatch one of the four streaming message kinds.

        Returns:
            The assembled `Transaction` for a `StreamCommit`, otherwise
            `None`.

        Raises:
            AssertionError: If `message` isn't one of the four streaming
                kinds.
        """
        match message:
            case StreamStart():
                self.stream_start(message)
            case StreamStop():
                self.stream_stop()
            case StreamCommit():
                return self.stream_commit(message)
            case StreamAbort():
                self.stream_abort(message)
            case _:  # pragma: no cover, exhaustive over the four streaming kinds
                unreachable = f"unhandled streaming message type {message!r}"
                raise AssertionError(unreachable)
        return None

    def _begin(self, message: Begin) -> None:
        if self._current_xid is not None:
            already_open = "received Begin while a transaction is already open"
            raise ProtocolError(
                already_open,
                context=ErrorContext(xid=message.xid),
            )
        if message.xid in self._pending:
            desync = (
                f"received Begin for xid {message.xid} "
                "which already has an open streaming bucket"
            )
            raise ProtocolError(desync, context=ErrorContext(xid=message.xid))
        self._current_xid = message.xid
        self._pending[message.xid] = _Pending(final_lsn=message.final_lsn)
        logger.debug("transaction opened", extra={"xid": message.xid})

    def _active_xid(self, message_kind: str) -> int:
        """Resolve which pending bucket a bare change message belongs to.

        Falls back from the streaming bracket currently receiving messages,
        if any, to the one open ordinary transaction, if any.

        Returns:
            The resolved xid.

        Raises:
            ProtocolError: If neither is open.
        """
        if self._active_stream_xid is not None:
            return self._active_stream_xid
        if self._current_xid is not None:
            return self._current_xid
        no_transaction = f"received {message_kind} with no open transaction"
        raise ProtocolError(no_transaction)

    def _append_change(
        self,
        xid: int,
        change: ChangeEvent,
        *,
        subxid: int | None,
    ) -> None:
        if xid not in self._pending:  # pragma: no cover, _active_xid already
            # guarantees a resolved xid has a `_pending` entry; this guards
            # `_append_change` itself against future misuse, not a path
            # reachable through `feed()` today.
            no_bucket = f"received a change for xid {xid} with no open bucket"
            raise ProtocolError(no_bucket, context=ErrorContext(xid=xid))
        pending = self._pending[xid]
        if subxid is not None and subxid not in pending.subxact_boundaries:
            pending.subxact_boundaries[subxid] = len(pending.changes)
        pending.changes.append(change)

    def _append_insert(self, message: Insert) -> None:
        xid = self._active_xid("Insert")
        self._append_change(
            xid,
            ChangeEvent(
                kind=ChangeKind.INSERT,
                table=message.table,
                new=message.new,
                old=None,
            ),
            subxid=message.subxid,
        )

    def _append_update(self, message: Update) -> None:
        xid = self._active_xid("Update")
        self._append_change(
            xid,
            ChangeEvent(
                kind=ChangeKind.UPDATE,
                table=message.table,
                new=message.new,
                old=message.old,
            ),
            subxid=message.subxid,
        )

    def _append_delete(self, message: Delete) -> None:
        xid = self._active_xid("Delete")
        self._append_change(
            xid,
            ChangeEvent(
                kind=ChangeKind.DELETE,
                table=message.table,
                new=None,
                old=message.old,
            ),
            subxid=message.subxid,
        )

    def _append_truncate(self, message: Truncate) -> None:
        xid = self._active_xid("Truncate")
        for table in message.tables:
            self._append_change(
                xid,
                ChangeEvent(kind=ChangeKind.TRUNCATE, table=table, new=None, old=None),
                subxid=message.subxid,
            )

    def _finish(self, message: Commit) -> Transaction:
        if self._current_xid is None:
            no_transaction = "received Commit with no open transaction"
            raise ProtocolError(no_transaction)
        xid = self._current_xid
        pending = self._pending[xid]
        if pending.final_lsn != message.commit_lsn:
            lsn_mismatch = "Begin.final_lsn does not match Commit.commit_lsn"
            raise ProtocolError(
                lsn_mismatch,
                context=ErrorContext(xid=xid, lsn=message.commit_lsn),
            )
        transaction = self._build_transaction(xid, pending.changes, message)
        del self._pending[xid]
        self._current_xid = None
        logger.debug(
            "transaction committed",
            extra={"xid": xid, "lsn": transaction.commit_lsn},
        )
        return transaction

    def stream_start(self, message: StreamStart) -> None:
        """Open (`first_segment=True`) or resume a streamed transaction's bucket.

        Raises:
            ProtocolError: On a `first_segment=True` for an already-open
                xid, or `first_segment=False` for an xid with no open
                bucket. Both indicate a protocol desync.
        """
        if message.first_segment:
            if message.xid in self._pending:
                duplicate = (
                    f"duplicate StreamStart(first_segment=True) for xid {message.xid}"
                )
                raise ProtocolError(duplicate, context=ErrorContext(xid=message.xid))
            self._pending[message.xid] = _Pending()
        elif message.xid not in self._pending:
            unknown = f"StreamStart continuation for unknown xid {message.xid}"
            raise ProtocolError(unknown, context=ErrorContext(xid=message.xid))
        self._active_stream_xid = message.xid

    def stream_stop(self) -> None:
        """Close the current chunk boundary; the bucket itself stays open."""
        self._active_stream_xid = None

    def stream_commit(self, message: StreamCommit) -> Transaction:
        """Complete a streamed transaction, emitting its `Transaction`.

        Returns:
            The assembled `Transaction`.

        Raises:
            ProtocolError: If `message.xid` has no open bucket.
        """
        if message.xid not in self._pending:
            no_transaction = f"received StreamCommit for unknown xid {message.xid}"
            raise ProtocolError(no_transaction, context=ErrorContext(xid=message.xid))
        pending = self._pending.pop(message.xid)
        if self._active_stream_xid == message.xid:
            self._active_stream_xid = None
        transaction = TransactionAssembler._build_transaction(
            message.xid,
            pending.changes,
            message,
        )
        logger.debug(
            "streamed transaction committed",
            extra={"xid": message.xid, "lsn": transaction.commit_lsn},
        )
        return transaction

    def stream_abort(self, message: StreamAbort) -> None:
        """Discard exactly the changes a `StreamAbort` invalidates.

        `subxid == xid` is a full-transaction rollback: the whole bucket is
        dropped. `subxid != xid` is a savepoint-level rollback: everything
        recorded from that subxid's first change onward (in
        `_Pending.subxact_boundaries`) is truncated away, since a
        subtransaction's changes are always contiguous. A `subxid` that
        never produced a change has no recorded boundary, so there's
        nothing to discard.
        """
        if message.subxid == message.xid:
            self._pending.pop(message.xid, None)
            if self._active_stream_xid == message.xid:
                self._active_stream_xid = None
            logger.debug("transaction aborted", extra={"xid": message.xid})
            return
        pending = self._pending.get(message.xid)
        if pending is None:
            # A subtransaction abort for a top-level xid we have no bucket
            # for at all, e.g. it already closed via a prior full abort or
            # StreamCommit. Nothing to discard.
            return
        boundary = pending.subxact_boundaries.pop(message.subxid, None)
        if boundary is None:
            logger.debug(
                "subtransaction aborted with no buffered changes to discard",
                extra={"xid": message.xid},
            )
            return
        discarded = len(pending.changes) - boundary
        del pending.changes[boundary:]
        # Any boundary recorded inside the discarded region belonged to a
        # savepoint nested within the one just aborted, already discarded
        # along with it, so its own record is now stale.
        stale = [
            subxid
            for subxid, index in pending.subxact_boundaries.items()
            if index >= boundary
        ]
        for subxid in stale:
            del pending.subxact_boundaries[subxid]
        logger.debug(
            "discarded %s change(s) from an aborted subtransaction",
            discarded,
            extra={"xid": message.xid},
        )

    @staticmethod
    def _build_transaction(
        xid: int,
        changes: list[ChangeEvent],
        commit: Commit | StreamCommit,
    ) -> Transaction:
        """Shared emission path: accumulated changes + Commit -> Transaction.

        Used by both `_finish` (an ordinary `Commit`) and `stream_commit`
        (a `StreamCommit`), which share the same `end_lsn`/`commit_time`
        field names.

        Returns:
            The assembled `Transaction`.
        """
        return Transaction(
            xid=xid,
            # commit.end_lsn is "the end of the commit record + 1" (Postgres's
            # own reorderbuffer.h), already in the wire protocol's "+1"
            # form. Every other LSN this codebase tracks is kept as a raw,
            # un-adjusted position, with "+1" applied exactly once at final
            # wire-encoding time, so subtracting 1 here converts end_lsn into
            # that same raw convention. commit.commit_lsn is the *start* of
            # the commit record, not its end: using it would make walbox
            # report having durably processed less than it actually has,
            # causing PostgreSQL to resend this transaction on every
            # reconnect. This matches PostgreSQL's own logical replication
            # apply worker (src/backend/replication/logical/worker.c,
            # apply_handle_commit_internal's store_flush_position call),
            # which uses end_lsn, never commit_lsn, for this purpose.
            commit_lsn=commit.end_lsn - 1,
            commit_time=commit.commit_time,
            changes=list(changes),  # defensive copy: outlives this assembler's state
        )
