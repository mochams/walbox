import pytest

from walbox.abc import ChangeKind
from walbox.errors import ProtocolError
from walbox.pgoutput import Begin
from walbox.pgoutput import Commit
from walbox.pgoutput import Truncate
from walbox.transaction import TransactionAssembler


def _begin(xid: int = 1, final_lsn: int = 100, commit_time: int = 111) -> Begin:
    return Begin(final_lsn=final_lsn, commit_time=commit_time, xid=xid)


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


def _truncate(
    tables: tuple[str, ...],
    *,
    cascade: bool = False,
    restart_identity: bool = False,
) -> Truncate:
    return Truncate(tables=tables, cascade=cascade, restart_identity=restart_identity)


def test_feed_truncate_fans_out_to_one_change_event_per_table():
    assembler = TransactionAssembler()
    assembler.feed(_begin())
    tables = ("public.a", "public.b", "public.c")
    assert assembler.feed(_truncate(tables)) is None
    transaction = assembler.feed(_commit())

    assert len(transaction.changes) == 3
    assert [c.kind for c in transaction.changes] == [ChangeKind.TRUNCATE] * 3
    assert [c.table for c in transaction.changes] == list(tables)
    for change in transaction.changes:
        assert change.new is None
        assert change.old is None


def test_feed_truncate_without_open_transaction_raises_protocol_error():
    assembler = TransactionAssembler()
    with pytest.raises(ProtocolError):
        assembler.feed(_truncate(("public.things",)))
