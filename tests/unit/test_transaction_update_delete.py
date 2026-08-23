import pytest

from walbox.abc import ChangeKind
from walbox.errors import ProtocolError
from walbox.pgoutput import Begin
from walbox.pgoutput import Commit
from walbox.pgoutput import Delete
from walbox.pgoutput import Update
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


def _update(
    table: str = "public.things",
    old: dict[str, str | None] | None = None,
    **new: str | None,
) -> Update:
    return Update(relation_id=1, table=table, old=old, new=new)


def _delete(table: str = "public.things", **old: str | None) -> Delete:
    return Delete(relation_id=1, table=table, old=old)


def test_feed_update_appends_a_change_event_with_kind_update():
    assembler = TransactionAssembler()
    assembler.feed(_begin())
    assert assembler.feed(_update(old={"id": "1"}, id="1", name="alice")) is None
    transaction = assembler.feed(_commit())
    assert len(transaction.changes) == 1
    change = transaction.changes[0]
    assert change.kind == ChangeKind.UPDATE
    assert change.table == "public.things"
    assert change.old == {"id": "1"}
    assert change.new == {"id": "1", "name": "alice"}


def test_feed_delete_appends_a_change_event_with_kind_delete_and_no_new():
    assembler = TransactionAssembler()
    assembler.feed(_begin())
    assert assembler.feed(_delete(id="1")) is None
    transaction = assembler.feed(_commit())
    assert len(transaction.changes) == 1
    change = transaction.changes[0]
    assert change.kind == ChangeKind.DELETE
    assert change.table == "public.things"
    assert change.new is None
    assert change.old == {"id": "1"}


def test_feed_update_without_open_transaction_raises_protocol_error():
    assembler = TransactionAssembler()
    with pytest.raises(ProtocolError):
        assembler.feed(_update(id="1"))


def test_feed_delete_without_open_transaction_raises_protocol_error():
    assembler = TransactionAssembler()
    with pytest.raises(ProtocolError):
        assembler.feed(_delete(id="1"))
