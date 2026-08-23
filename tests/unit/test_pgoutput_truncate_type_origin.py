import pytest

from walbox.errors import DecodeError
from walbox.pgoutput import Column
from walbox.pgoutput import Decoder
from walbox.pgoutput import Insert
from walbox.pgoutput import Origin
from walbox.pgoutput import Relation
from walbox.pgoutput import RelationCache
from walbox.pgoutput import Truncate
from walbox.pgoutput import Type
from walbox.pgoutput import decode_origin
from walbox.pgoutput import decode_truncate
from walbox.pgoutput import decode_type


def _things_relation(relation_id: int = 1) -> Relation:
    return Relation(
        relation_id=relation_id,
        namespace="public",
        name="things",
        replica_identity="d",
        columns=(Column(name="id", type_oid=23, type_modifier=-1, is_key=True),),
    )


def _cstring(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"


def _relation_bytes(relation_id: int, namespace: str, name: str) -> bytes:
    columns = [("id", 23, -1, True)]
    body = (
        b"R"
        + relation_id.to_bytes(4, "big")
        + _cstring(namespace)
        + _cstring(name)
        + b"d"
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


def _insert_bytes(relation_id: int) -> bytes:
    return (
        b"I"
        + relation_id.to_bytes(4, "big")
        + b"N"
        + (1).to_bytes(2, "big")
        + b"t"
        + (1).to_bytes(4, "big")
        + b"5"
    )


def _truncate_bytes(relation_ids: list[int], *, flags: int = 0) -> bytes:
    body = b"T" + len(relation_ids).to_bytes(4, "big") + bytes([flags])
    for relation_id in relation_ids:
        body += relation_id.to_bytes(4, "big")
    return body


def _type_bytes(type_oid: int, namespace: str, name: str) -> bytes:
    return b"Y" + type_oid.to_bytes(4, "big") + _cstring(namespace) + _cstring(name)


def _origin_bytes(origin_lsn: int, name: str) -> bytes:
    return b"O" + origin_lsn.to_bytes(8, "big") + _cstring(name)


def test_decode_truncate_single_relation_no_flags():
    relations = RelationCache()
    relations.add(_things_relation(1))
    truncate = decode_truncate(_truncate_bytes([1]), relations)
    assert truncate.tables == ("public.things",)
    assert truncate.cascade is False
    assert truncate.restart_identity is False


def test_decode_truncate_multiple_relations_resolves_all_table_names():
    relations = RelationCache()
    relations.add(_things_relation(1))
    relations.add(Relation(2, "public", "others", "d", ()))
    relations.add(Relation(3, "public", "more", "d", ()))
    truncate = decode_truncate(_truncate_bytes([3, 1, 2]), relations)
    assert truncate.tables == ("public.more", "public.things", "public.others")


def test_decode_truncate_cascade_and_restart_identity_flags():
    relations = RelationCache()
    relations.add(_things_relation(1))
    truncate = decode_truncate(_truncate_bytes([1], flags=3), relations)
    assert truncate.cascade is True
    assert truncate.restart_identity is True


def test_decode_truncate_unknown_relation_id_raises_decode_error():
    relations = RelationCache()
    with pytest.raises(DecodeError):
        decode_truncate(_truncate_bytes([999]), relations)


def test_decode_truncate_rejects_wrong_leading_byte():
    with pytest.raises(DecodeError):
        decode_truncate(b"X" + b"\x00" * 10, RelationCache())


def test_decode_type_message():
    decoded = decode_type(_type_bytes(16400, "public", "my_enum"))
    assert decoded == Type(type_oid=16400, namespace="public", name="my_enum")


def test_decode_type_rejects_wrong_leading_byte():
    with pytest.raises(DecodeError):
        decode_type(b"X" + b"\x00" * 10)


def test_decode_origin_message():
    decoded = decode_origin(_origin_bytes(12345, "my_origin"))
    assert decoded == Origin(origin_lsn=12345, name="my_origin")


def test_decode_origin_rejects_wrong_leading_byte():
    with pytest.raises(DecodeError):
        decode_origin(b"X" + b"\x00" * 10)


def test_decode_message_dispatches_truncate():
    decoder = Decoder()
    decoder.decode(_relation_bytes(7, "public", "widgets"))

    truncate = decoder.decode(_truncate_bytes([7]))
    assert isinstance(truncate, Truncate)
    assert truncate.tables == ("public.widgets",)


def test_decode_type_message_does_not_desync_stream():
    decoder = Decoder()
    decoder.decode(_relation_bytes(7, "public", "widgets"))

    decoded_type = decoder.decode(_type_bytes(16400, "public", "my_enum"))
    assert isinstance(decoded_type, Type)

    insert = decoder.decode(_insert_bytes(7))
    assert isinstance(insert, Insert)
    assert insert.table == "public.widgets"
    assert insert.new == {"id": "5"}


def test_decode_origin_message_does_not_desync_stream():
    decoder = Decoder()
    decoder.decode(_relation_bytes(7, "public", "widgets"))

    decoded_origin = decoder.decode(_origin_bytes(999, "my_origin"))
    assert isinstance(decoded_origin, Origin)

    insert = decoder.decode(_insert_bytes(7))
    assert isinstance(insert, Insert)
    assert insert.table == "public.widgets"
    assert insert.new == {"id": "5"}


def test_relation_cache_overwritten_on_redefinition():
    decoder = Decoder()
    decoder.decode(_relation_bytes(7, "public", "widgets"))

    new_relation_bytes = (
        b"R"
        + (7).to_bytes(4, "big")
        + _cstring("public")
        + _cstring("widgets")
        + b"d"
        + (2).to_bytes(2, "big")
        + bytes([1])
        + _cstring("id")
        + (23).to_bytes(4, "big")
        + (-1).to_bytes(4, "big", signed=True)
        + bytes([0])
        + _cstring("extra")
        + (25).to_bytes(4, "big")
        + (-1).to_bytes(4, "big", signed=True)
    )
    decoder.decode(new_relation_bytes)

    insert_raw = (
        b"I"
        + (7).to_bytes(4, "big")
        + b"N"
        + (2).to_bytes(2, "big")
        + b"t"
        + (1).to_bytes(4, "big")
        + b"5"
        + b"t"
        + (3).to_bytes(4, "big")
        + b"foo"
    )
    insert = decoder.decode(insert_raw)
    assert isinstance(insert, Insert)
    assert insert.new == {"id": "5", "extra": "foo"}
