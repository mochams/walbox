import pytest

from walbox.errors import DecodeError
from walbox.pgoutput import Begin
from walbox.pgoutput import Column
from walbox.pgoutput import Commit
from walbox.pgoutput import Decoder
from walbox.pgoutput import Insert
from walbox.pgoutput import Relation
from walbox.pgoutput import RelationCache
from walbox.pgoutput import decode_begin
from walbox.pgoutput import decode_commit
from walbox.pgoutput import decode_insert
from walbox.pgoutput import decode_relation


def _cstring(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"


def _relation_bytes(
    relation_id: int,
    namespace: str,
    name: str,
    replica_identity: str,
    columns: list[tuple[str, int, int, bool]],
) -> bytes:
    body = (
        b"R"
        + relation_id.to_bytes(4, "big")
        + _cstring(namespace)
        + _cstring(name)
        + replica_identity.encode("ascii")
        + len(columns).to_bytes(2, "big")
    )
    for col_name, type_oid, type_modifier, is_key in columns:
        body += (
            bytes([1 if is_key else 0])
            + _cstring(col_name)
            + type_oid.to_bytes(4, "big")
            + type_modifier.to_bytes(4, "big", signed=True)
        )
    return body


def _begin_bytes(final_lsn: int, commit_time: int, xid: int) -> bytes:
    return (
        b"B"
        + final_lsn.to_bytes(8, "big")
        + commit_time.to_bytes(8, "big")
        + xid.to_bytes(4, "big")
    )


def _commit_bytes(flags: int, commit_lsn: int, end_lsn: int, commit_time: int) -> bytes:
    return (
        b"C"
        + bytes([flags])
        + commit_lsn.to_bytes(8, "big")
        + end_lsn.to_bytes(8, "big")
        + commit_time.to_bytes(8, "big")
    )


def _tuple_data(values: list[tuple[str, str | None]]) -> bytes:
    body = len(values).to_bytes(2, "big")
    for _, value in values:
        if value is None:
            body += b"n"
        else:
            encoded = value.encode("utf-8")
            body += b"t" + len(encoded).to_bytes(4, "big") + encoded
    return body


def _insert_bytes(relation_id: int, values: list[tuple[str, str | None]]) -> bytes:
    return b"I" + relation_id.to_bytes(4, "big") + b"N" + _tuple_data(values)


def _things_relation(relation_id: int = 1) -> Relation:
    return Relation(
        relation_id=relation_id,
        namespace="public",
        name="things",
        replica_identity="d",
        columns=(
            Column(name="id", type_oid=23, type_modifier=-1, is_key=True),
            Column(name="name", type_oid=25, type_modifier=-1, is_key=False),
            Column(name="amount", type_oid=1700, type_modifier=-1, is_key=False),
        ),
    )


def test_decode_relation_parses_namespace_name_identity_and_columns():
    raw = _relation_bytes(
        relation_id=42,
        namespace="public",
        name="things",
        replica_identity="d",
        columns=[
            ("id", 23, -1, True),
            ("name", 25, 100, False),
        ],
    )
    relation = decode_relation(raw)
    assert relation.relation_id == 42
    assert relation.namespace == "public"
    assert relation.name == "things"
    assert relation.replica_identity == "d"
    assert relation.qualified_name == "public.things"
    assert relation.columns == (
        Column(name="id", type_oid=23, type_modifier=-1, is_key=True),
        Column(name="name", type_oid=25, type_modifier=100, is_key=False),
    )


def test_decode_relation_rejects_wrong_leading_byte():
    with pytest.raises(DecodeError):
        decode_relation(b"X" + b"\x00" * 10)


def test_decode_begin_parses_final_lsn_commit_time_and_xid():
    raw = _begin_bytes(final_lsn=111, commit_time=222, xid=333)
    begin = decode_begin(raw)
    assert begin.final_lsn == 111
    assert begin.commit_time == 222
    assert begin.xid == 333


def test_decode_begin_rejects_wrong_leading_byte():
    with pytest.raises(DecodeError):
        decode_begin(b"X" + b"\x00" * 20)


def test_decode_begin_rejects_wrong_length():
    with pytest.raises(DecodeError):
        decode_begin(_begin_bytes(1, 2, 3) + b"\x00")


def test_decode_commit_parses_all_four_fields_with_distinct_lsns():
    raw = _commit_bytes(flags=0, commit_lsn=100, end_lsn=200, commit_time=300)
    commit = decode_commit(raw)
    assert commit.flags == 0
    assert commit.commit_lsn == 100
    assert commit.end_lsn == 200
    assert commit.commit_time == 300


def test_decode_commit_rejects_wrong_leading_byte():
    with pytest.raises(DecodeError):
        decode_commit(b"X" + b"\x00" * 25)


def test_decode_commit_rejects_wrong_length():
    with pytest.raises(DecodeError):
        decode_commit(_commit_bytes(0, 1, 2, 3) + b"\x00")


def test_decode_insert_builds_a_named_dict_using_relation_columns():
    relations = RelationCache()
    relations.add(_things_relation())
    raw = _insert_bytes(
        relation_id=1,
        values=[("id", "1"), ("name", "alice"), ("amount", "9.99")],
    )
    insert = decode_insert(raw, relations)
    assert insert.new == {"id": "1", "name": "alice", "amount": "9.99"}
    assert insert.table == "public.things"
    assert insert.relation_id == 1


def test_decode_insert_maps_null_marker_to_none():
    relations = RelationCache()
    relations.add(_things_relation())
    raw = _insert_bytes(
        relation_id=1,
        values=[("id", "1"), ("name", None), ("amount", "9.99")],
    )
    insert = decode_insert(raw, relations)
    assert insert.new["name"] is None


def test_decode_insert_unknown_relation_raises_decode_error():
    relations = RelationCache()
    raw = _insert_bytes(relation_id=999, values=[("id", "1")])
    with pytest.raises(DecodeError):
        decode_insert(raw, relations)


def test_decode_insert_column_count_mismatch_raises_decode_error():
    relations = RelationCache()
    relations.add(_things_relation())
    raw = _insert_bytes(relation_id=1, values=[("id", "1")])
    with pytest.raises(DecodeError):
        decode_insert(raw, relations)


def test_decode_insert_rejects_wrong_leading_byte():
    with pytest.raises(DecodeError):
        decode_insert(b"X" + b"\x00" * 10, RelationCache())


def test_decode_insert_rejects_missing_new_tuple_marker():
    relations = RelationCache()
    relations.add(_things_relation())
    raw = b"I" + (1).to_bytes(4, "big") + b"X" + _tuple_data([("id", "1")])
    with pytest.raises(DecodeError):
        decode_insert(raw, relations)


def test_decoder_uses_cached_schema_from_a_prior_relation_message():
    decoder = Decoder()
    relation_raw = _relation_bytes(
        relation_id=7,
        namespace="public",
        name="widgets",
        replica_identity="d",
        columns=[("id", 23, -1, True)],
    )
    decoder.decode(relation_raw)
    insert_raw = _insert_bytes(relation_id=7, values=[("id", "5")])
    insert = decoder.decode(insert_raw)
    assert isinstance(insert, Insert)
    assert insert.new == {"id": "5"}
    assert insert.table == "public.widgets"


def test_decoder_returns_the_relation_object_itself():
    decoder = Decoder()
    relation_raw = _relation_bytes(
        relation_id=1,
        namespace="public",
        name="things",
        replica_identity="d",
        columns=[("id", 23, -1, True)],
    )
    decoded = decoder.decode(relation_raw)
    assert isinstance(decoded, Relation)


def test_decoder_unsupported_message_type_raises_decode_error():
    # 'X' isn't a real pgoutput message kind at all, unlike every other
    # single-letter tag walbox now decodes ('T'/'Y'/'O'/'M' included).
    decoder = Decoder()
    with pytest.raises(DecodeError):
        decoder.decode(b"X" + b"\x00" * 10)


def test_decoder_dispatches_begin_messages():
    decoder = Decoder()
    raw = _begin_bytes(final_lsn=1, commit_time=2, xid=3)
    assert isinstance(decoder.decode(raw), Begin)


def test_decoder_dispatches_commit_messages():
    decoder = Decoder()
    raw = _commit_bytes(flags=0, commit_lsn=1, end_lsn=2, commit_time=3)
    assert isinstance(decoder.decode(raw), Commit)


def test_decode_insert_unchanged_toasted_marker_omits_column():
    relations = RelationCache()
    relations.add(_things_relation())
    raw = b"I" + (1).to_bytes(4, "big") + b"N" + (3).to_bytes(2, "big")
    raw += b"t" + (1).to_bytes(4, "big") + b"1"
    raw += b"u"
    raw += b"t" + (4).to_bytes(4, "big") + b"9.99"
    insert = decode_insert(raw, relations)
    assert "name" not in insert.new


def test_decode_insert_unknown_tuple_data_marker_raises_decode_error():
    relations = RelationCache()
    relations.add(_things_relation())
    raw = b"I" + (1).to_bytes(4, "big") + b"N" + (3).to_bytes(2, "big")
    raw += b"t" + (1).to_bytes(4, "big") + b"1"
    raw += b"z"
    raw += b"t" + (4).to_bytes(4, "big") + b"9.99"
    with pytest.raises(DecodeError):
        decode_insert(raw, relations)
