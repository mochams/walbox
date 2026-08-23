"""Unit tests for streamed-transaction assembly.

Covers the keyed `_pending` buffer: interleaving streamed transactions'
chunks, ordinary transactions delivered in the gaps, `StreamAbort`'s
full-transaction vs. subxid handling, and the `StreamCommit` LSN mapping
that must match ordinary `Commit`'s exactly.
"""

import pytest

from walbox.errors import ProtocolError
from walbox.pgoutput import Begin
from walbox.pgoutput import Commit
from walbox.pgoutput import Insert
from walbox.pgoutput import StreamAbort
from walbox.pgoutput import StreamCommit
from walbox.pgoutput import StreamStart
from walbox.pgoutput import StreamStop
from walbox.transaction import TransactionAssembler


def _begin(xid: int = 1, final_lsn: int = 100, commit_time: int = 111) -> Begin:
    return Begin(final_lsn=final_lsn, commit_time=commit_time, xid=xid)


def _insert(
    table: str = "public.things",
    *,
    subxid: int | None = None,
    **new: str | None,
) -> Insert:
    return Insert(relation_id=1, table=table, new=new, subxid=subxid)


def _commit(
    commit_lsn: int = 100,
    end_lsn: int = 150,
    commit_time: int = 222,
    flags: int = 0,
) -> Commit:
    return Commit(
        flags=flags,
        commit_lsn=commit_lsn,
        end_lsn=end_lsn,
        commit_time=commit_time,
    )


def _stream_start(xid: int, *, first_segment: bool) -> StreamStart:
    return StreamStart(xid=xid, first_segment=first_segment)


def _stream_stop() -> StreamStop:
    return StreamStop()


def _stream_commit(
    xid: int,
    commit_lsn: int = 100,
    end_lsn: int = 150,
    commit_time: int = 222,
) -> StreamCommit:
    return StreamCommit(
        xid=xid,
        commit_lsn=commit_lsn,
        end_lsn=end_lsn,
        commit_time=commit_time,
    )


def _stream_abort(xid: int, subxid: int) -> StreamAbort:
    return StreamAbort(xid=xid, subxid=subxid)


def test_stream_start_creates_pending_bucket():
    assembler = TransactionAssembler()
    assembler.feed(_stream_start(1, first_segment=True))
    assembler.feed(_insert(id="1"))
    transaction = assembler.feed(_stream_commit(1))
    assert transaction.xid == 1
    assert [c.new["id"] for c in transaction.changes] == ["1"]


def test_stream_start_duplicate_first_segment_raises_protocol_error():
    assembler = TransactionAssembler()
    assembler.feed(_stream_start(1, first_segment=True))
    with pytest.raises(ProtocolError):
        assembler.feed(_stream_start(1, first_segment=True))


def test_stream_start_continuation_requires_existing_bucket():
    assembler = TransactionAssembler()
    with pytest.raises(ProtocolError):
        assembler.feed(_stream_start(1, first_segment=False))


def test_stream_start_continuation_resumes_an_existing_bucket():
    assembler = TransactionAssembler()
    assembler.feed(_stream_start(1, first_segment=True))
    assembler.feed(_insert(id="1"))
    assembler.feed(_stream_stop())

    assembler.feed(_stream_start(1, first_segment=False))
    assembler.feed(_insert(id="2"))
    transaction = assembler.feed(_stream_commit(1))

    assert [c.new["id"] for c in transaction.changes] == ["1", "2"]


def test_interleaved_streamed_transactions_do_not_cross_contaminate():
    assembler = TransactionAssembler()

    assembler.feed(_stream_start(1, first_segment=True))
    assembler.feed(_insert(id="a1"))
    assembler.feed(_stream_stop())

    assembler.feed(_stream_start(2, first_segment=True))
    assembler.feed(_insert(id="b1"))
    assembler.feed(_stream_stop())

    assembler.feed(_stream_start(1, first_segment=False))
    assembler.feed(_insert(id="a2"))
    assembler.feed(_stream_stop())

    first = assembler.feed(_stream_commit(1, commit_lsn=100, end_lsn=150))
    second = assembler.feed(_stream_commit(2, commit_lsn=200, end_lsn=250))

    assert first.xid == 1
    assert [c.new["id"] for c in first.changes] == ["a1", "a2"]
    assert second.xid == 2
    assert [c.new["id"] for c in second.changes] == ["b1"]


def test_ordinary_transaction_interleaved_with_streamed_one():
    assembler = TransactionAssembler()

    assembler.feed(_stream_start(1, first_segment=True))
    assembler.feed(_insert(id="stream-1"))
    assembler.feed(_stream_stop())

    assembler.feed(_begin(xid=2, final_lsn=200))
    assembler.feed(_insert(id="ordinary-1"))
    ordinary = assembler.feed(_commit(commit_lsn=200, end_lsn=250))

    assembler.feed(_stream_start(1, first_segment=False))
    assembler.feed(_insert(id="stream-2"))
    streamed = assembler.feed(_stream_commit(1, commit_lsn=100, end_lsn=150))

    assert ordinary.xid == 2
    assert [c.new["id"] for c in ordinary.changes] == ["ordinary-1"]
    assert streamed.xid == 1
    assert [c.new["id"] for c in streamed.changes] == ["stream-1", "stream-2"]


def test_stream_abort_full_transaction_discards_buffer():
    assembler = TransactionAssembler()
    assembler.feed(_stream_start(1, first_segment=True))
    assembler.feed(_insert(id="1"))
    assembler.feed(_stream_abort(1, 1))

    with pytest.raises(ProtocolError):
        assembler.feed(_stream_commit(1))


def test_stream_abort_full_transaction_after_stream_stop_discards_buffer():
    """The abort can arrive once the bracket is no longer "active" (after a
    `StreamStop`, before the next `StreamStart` continuation) -- the buffer
    is still discarded correctly.
    """
    assembler = TransactionAssembler()
    assembler.feed(_stream_start(1, first_segment=True))
    assembler.feed(_insert(id="1"))
    assembler.feed(_stream_stop())
    assembler.feed(_stream_abort(1, 1))

    with pytest.raises(ProtocolError):
        assembler.feed(_stream_commit(1))


def test_stream_abort_full_transaction_allows_reusing_the_xid():
    assembler = TransactionAssembler()
    assembler.feed(_stream_start(1, first_segment=True))
    assembler.feed(_insert(id="doomed"))
    assembler.feed(_stream_abort(1, 1))

    assembler.feed(_stream_start(1, first_segment=True))
    assembler.feed(_insert(id="retry"))
    transaction = assembler.feed(_stream_commit(1))

    assert [c.new["id"] for c in transaction.changes] == ["retry"]


def test_stream_abort_subtransaction_discards_only_that_subtransactions_changes():
    """A `ROLLBACK TO SAVEPOINT` discards exactly the changes made under it.

    Every row-change message carries its own (sub)transaction's id while
    streaming (`Insert.subxid`); a change made directly in the top-level
    transaction carries the top-level xid itself. Changes before and after
    the savepoint survive; only the ones made under it are discarded.
    """
    assembler = TransactionAssembler()
    assembler.feed(_stream_start(1, first_segment=True))
    assembler.feed(_insert(id="before", subxid=1))
    assembler.feed(_insert(id="doomed", subxid=99))
    assembler.feed(_stream_abort(1, 99))
    assembler.feed(_insert(id="after", subxid=1))
    transaction = assembler.feed(_stream_commit(1))

    assert [c.new["id"] for c in transaction.changes] == ["before", "after"]


def test_stream_abort_subtransaction_with_no_buffered_changes_is_a_no_op():
    """`SAVEPOINT x; ROLLBACK TO SAVEPOINT x;` with no writes in between.

    No change was ever recorded under `subxid=2`, so there is no boundary
    to discard from -- the buffer is untouched.
    """
    assembler = TransactionAssembler()
    assembler.feed(_stream_start(1, first_segment=True))
    assembler.feed(_insert(id="1", subxid=1))
    assembler.feed(_stream_abort(1, 2))
    assembler.feed(_insert(id="2", subxid=1))
    transaction = assembler.feed(_stream_commit(1))

    assert [c.new["id"] for c in transaction.changes] == ["1", "2"]


def test_stream_abort_subtransaction_discards_nested_savepoints_too():
    """Rolling back an outer savepoint discards everything nested inside it.

    `ROLLBACK TO SAVEPOINT a` implicitly discards savepoint `b`'s changes
    too, in the same operation -- Postgres does not send a separate
    `StreamAbort` for `b`. Truncating the buffer back to `a`'s own boundary
    already covers both, since `b`'s changes are chronologically after it.
    """
    assembler = TransactionAssembler()
    assembler.feed(_stream_start(1, first_segment=True))
    assembler.feed(_insert(id="before", subxid=1))
    assembler.feed(_insert(id="in-a", subxid=50))
    assembler.feed(_insert(id="in-b-nested-in-a", subxid=51))
    assembler.feed(_stream_abort(1, 50))
    assembler.feed(_insert(id="after", subxid=1))
    transaction = assembler.feed(_stream_commit(1))

    assert [c.new["id"] for c in transaction.changes] == ["before", "after"]


def test_stream_abort_subtransaction_for_unknown_top_level_xid_is_a_no_op():
    assembler = TransactionAssembler()
    assembler.stream_abort(_stream_abort(1, 2))


def test_stream_commit_for_unknown_xid_raises_protocol_error():
    assembler = TransactionAssembler()
    with pytest.raises(ProtocolError):
        assembler.feed(_stream_commit(1))


def test_stream_commit_derives_transaction_commit_lsn_from_end_lsn_minus_one():
    assembler = TransactionAssembler()
    assembler.feed(_stream_start(1, first_segment=True))
    assembler.feed(_insert(id="1"))
    transaction = assembler.feed(
        _stream_commit(1, commit_lsn=100, end_lsn=150, commit_time=222),
    )

    assert transaction.commit_lsn == 149
    assert transaction.commit_time == 222


def test_insert_inside_streaming_bracket_with_no_bucket_raises_protocol_error():
    assembler = TransactionAssembler()
    with pytest.raises(ProtocolError):
        assembler.feed(_insert(id="1"))


def test_begin_for_xid_with_open_streaming_bucket_raises_protocol_error():
    assembler = TransactionAssembler()
    assembler.feed(_stream_start(1, first_segment=True))
    with pytest.raises(ProtocolError):
        assembler.feed(_begin(xid=1))
