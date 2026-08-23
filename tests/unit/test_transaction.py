import pytest

from walbox.errors import ProtocolError
from walbox.pgoutput import Begin
from walbox.pgoutput import Column
from walbox.pgoutput import Commit
from walbox.pgoutput import Insert
from walbox.pgoutput import Relation
from walbox.transaction import TransactionAssembler


def _begin(xid: int = 1, final_lsn: int = 100, commit_time: int = 111) -> Begin:
    return Begin(final_lsn=final_lsn, commit_time=commit_time, xid=xid)


def _insert(table: str = "public.things", **new: str | None) -> Insert:
    return Insert(relation_id=1, table=table, new=new)


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


def _relation() -> Relation:
    return Relation(
        relation_id=1,
        namespace="public",
        name="things",
        replica_identity="d",
        columns=(Column(name="id", type_oid=23, type_modifier=-1, is_key=True),),
    )


def test_feed_returns_none_for_begin_and_insert():
    assembler = TransactionAssembler()
    assert assembler.feed(_begin()) is None
    assert assembler.feed(_insert(id="1")) is None
    assert assembler.feed(_insert(id="2")) is None


def test_feed_returns_a_transaction_on_commit():
    assembler = TransactionAssembler()
    assembler.feed(_begin(xid=42, final_lsn=100))
    assembler.feed(_insert(id="1"))
    assembler.feed(_insert(id="2"))
    commit = _commit(commit_lsn=100, end_lsn=150, commit_time=222)
    transaction = assembler.feed(commit)

    assert transaction.xid == 42
    assert transaction.commit_time == commit.commit_time
    assert transaction.commit_lsn == commit.end_lsn - 1
    assert len(transaction.changes) == 2
    assert [change.new["id"] for change in transaction.changes] == ["1", "2"]


def test_commit_lsn_is_derived_from_end_lsn_minus_one_not_commit_lsn():
    assembler = TransactionAssembler()
    assembler.feed(_begin(final_lsn=100))
    transaction = assembler.feed(_commit(commit_lsn=100, end_lsn=150))

    assert transaction.commit_lsn == 149


def test_relation_message_is_ignored_without_disturbing_open_transaction_state():
    assembler = TransactionAssembler()
    assembler.feed(_begin(final_lsn=100))
    assert assembler.feed(_relation()) is None
    assembler.feed(_insert(id="1"))
    transaction = assembler.feed(_commit(commit_lsn=100))

    assert len(transaction.changes) == 1
    assert transaction.changes[0].new["id"] == "1"


def test_insert_without_open_transaction_raises_protocol_error():
    assembler = TransactionAssembler()
    with pytest.raises(ProtocolError):
        assembler.feed(_insert(id="1"))


def test_commit_without_open_transaction_raises_protocol_error():
    assembler = TransactionAssembler()
    with pytest.raises(ProtocolError):
        assembler.feed(_commit())


def test_begin_while_a_transaction_is_already_open_raises_protocol_error():
    assembler = TransactionAssembler()
    assembler.feed(_begin())
    with pytest.raises(ProtocolError):
        assembler.feed(_begin())


def test_begin_final_lsn_mismatch_with_commit_lsn_raises_protocol_error():
    assembler = TransactionAssembler()
    assembler.feed(_begin(final_lsn=100))
    with pytest.raises(ProtocolError):
        assembler.feed(_commit(commit_lsn=999))


def test_assembler_resets_after_commit_and_handles_a_second_transaction_independently():
    assembler = TransactionAssembler()
    assembler.feed(_begin(xid=1, final_lsn=100))
    assembler.feed(_insert(id="1"))
    first = assembler.feed(_commit(commit_lsn=100))

    assembler.feed(_begin(xid=2, final_lsn=200))
    assembler.feed(_insert(id="2"))
    assembler.feed(_insert(id="3"))
    second = assembler.feed(_commit(commit_lsn=200))

    assert first.xid == 1
    assert [change.new["id"] for change in first.changes] == ["1"]
    assert second.xid == 2
    assert [change.new["id"] for change in second.changes] == ["2", "3"]


def test_emitted_transaction_is_independent_of_the_internal_buffer():
    assembler = TransactionAssembler()
    assembler.feed(_begin(xid=1, final_lsn=100))
    assembler.feed(_insert(id="1"))
    first = assembler.feed(_commit(commit_lsn=100))

    assembler.feed(_begin(xid=2, final_lsn=200))
    assembler.feed(_insert(id="2"))
    assembler.feed(_insert(id="3"))
    assembler.feed(_commit(commit_lsn=200))

    assert len(first.changes) == 1
    assert first.changes[0].new["id"] == "1"


def test_multiple_transactions_are_returned_in_commit_order():
    assembler = TransactionAssembler()
    assembler.feed(_begin(xid=1, final_lsn=100))
    first = assembler.feed(_commit(commit_lsn=100))

    assembler.feed(_begin(xid=2, final_lsn=200))
    second = assembler.feed(_commit(commit_lsn=200))

    assert [first.xid, second.xid] == [1, 2]
