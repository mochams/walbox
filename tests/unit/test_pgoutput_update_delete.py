import pytest

from walbox.errors import DecodeError
from walbox.pgoutput import Column
from walbox.pgoutput import Decoder
from walbox.pgoutput import Delete
from walbox.pgoutput import Relation
from walbox.pgoutput import RelationCache
from walbox.pgoutput import Update
from walbox.pgoutput import decode_delete
from walbox.pgoutput import decode_update


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


def _tuple_data(
    values: list[tuple[str, str | None]],
    *,
    unchanged: set[str] = frozenset(),
) -> bytes:
    body = len(values).to_bytes(2, "big")
    for name, value in values:
        if name in unchanged:
            body += b"u"
        elif value is None:
            body += b"n"
        else:
            encoded = value.encode("utf-8")
            body += b"t" + len(encoded).to_bytes(4, "big") + encoded
    return body


def _relation_bytes(relation_id: int, namespace: str, name: str) -> bytes:
    def _cstring(value: str) -> bytes:
        return value.encode("utf-8") + b"\x00"

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


def _update_bytes(
    relation_id: int,
    *,
    old_marker: bytes | None,
    old: list[tuple[str, str | None]] | None = None,
    new: list[tuple[str, str | None]],
) -> bytes:
    body = b"U" + relation_id.to_bytes(4, "big")
    if old_marker is not None:
        body += old_marker + _tuple_data(old or [])
    body += b"N" + _tuple_data(new)
    return body


def _delete_bytes(
    relation_id: int,
    marker: bytes,
    old: list[tuple[str, str | None]],
    *,
    unchanged: set[str] = frozenset(),
) -> bytes:
    return (
        b"D"
        + relation_id.to_bytes(4, "big")
        + marker
        + _tuple_data(old, unchanged=unchanged)
    )


def test_decode_update_full_replica_identity():
    relations = RelationCache()
    relations.add(_things_relation())
    raw = _update_bytes(
        relation_id=1,
        old_marker=b"O",
        old=[("id", "1"), ("name", "alice"), ("amount", "9.99")],
        new=[("id", "1"), ("name", "alice2"), ("amount", "19.99")],
    )
    update = decode_update(raw, relations)
    assert update.relation_id == 1
    assert update.table == "public.things"
    assert update.old == {"id": "1", "name": "alice", "amount": "9.99"}
    assert update.new == {"id": "1", "name": "alice2", "amount": "19.99"}


def test_decode_update_key_only_on_key_change():
    # Real Postgres marks every non-key column 'n' (the plain null marker) in
    # a 'K' tuple, not 'u' -- the two are indistinguishable on the wire, so
    # decode_update must filter down to key columns itself, using the cached
    # relation's `is_key` flags, rather than relying on marker bytes alone.
    relations = RelationCache()
    relations.add(_things_relation())
    raw = _update_bytes(
        relation_id=1,
        old_marker=b"K",
        old=[("id", "1"), ("name", None), ("amount", None)],
        new=[("id", "2"), ("name", "alice"), ("amount", "9.99")],
    )
    update = decode_update(raw, relations)
    assert update.old == {"id": "1"}
    assert update.new == {"id": "2", "name": "alice", "amount": "9.99"}


def test_decode_update_no_old_tuple_when_key_unchanged():
    relations = RelationCache()
    relations.add(_things_relation())
    raw = _update_bytes(
        relation_id=1,
        old_marker=None,
        old=None,
        new=[("id", "1"), ("name", "alice2"), ("amount", "19.99")],
    )
    update = decode_update(raw, relations)
    assert update.old is None
    assert update.new == {"id": "1", "name": "alice2", "amount": "19.99"}


def test_decode_update_missing_new_tuple_marker_raises_decode_error():
    relations = RelationCache()
    relations.add(_things_relation())
    raw = b"U" + (1).to_bytes(4, "big") + b"X" + _tuple_data([("id", "1")])
    with pytest.raises(DecodeError):
        decode_update(raw, relations)


def test_decode_update_rejects_wrong_leading_byte():
    with pytest.raises(DecodeError):
        decode_update(b"X" + b"\x00" * 10, RelationCache())


def test_decode_delete_key_only():
    # Same real-wire nuance as the Update case: non-key columns arrive 'n',
    # and decode_delete must filter them out itself.
    relations = RelationCache()
    relations.add(_things_relation())
    raw = _delete_bytes(
        1,
        b"K",
        [("id", "1"), ("name", None), ("amount", None)],
    )
    delete = decode_delete(raw, relations)
    assert delete.relation_id == 1
    assert delete.table == "public.things"
    assert delete.old == {"id": "1"}


def test_decode_delete_full_replica_identity():
    relations = RelationCache()
    relations.add(_things_relation())
    raw = _delete_bytes(
        1,
        b"O",
        [("id", "1"), ("name", "alice"), ("amount", "9.99")],
    )
    delete = decode_delete(raw, relations)
    assert delete.old == {"id": "1", "name": "alice", "amount": "9.99"}


def test_decode_delete_missing_marker_raises_decode_error():
    relations = RelationCache()
    relations.add(_things_relation())
    raw = b"D" + (1).to_bytes(4, "big") + b"X" + _tuple_data([("id", "1")])
    with pytest.raises(DecodeError):
        decode_delete(raw, relations)


def test_decode_delete_rejects_wrong_leading_byte():
    with pytest.raises(DecodeError):
        decode_delete(b"X" + b"\x00" * 10, RelationCache())


def test_tuple_unchanged_toast_marker_omits_column():
    relations = RelationCache()
    relations.add(_things_relation())
    raw = _delete_bytes(
        1,
        b"O",
        [("id", "1"), ("name", None), ("amount", None)],
        unchanged={"amount"},
    )
    delete = decode_delete(raw, relations)
    assert delete.old["id"] == "1"
    assert delete.old["name"] is None
    assert "amount" not in delete.old


def test_update_unknown_relation_id_raises_decode_error():
    relations = RelationCache()
    raw = _update_bytes(
        relation_id=999,
        old_marker=None,
        old=None,
        new=[("id", "1")],
    )
    with pytest.raises(DecodeError):
        decode_update(raw, relations)


def test_delete_unknown_relation_id_raises_decode_error():
    relations = RelationCache()
    raw = _delete_bytes(999, b"K", [("id", "1")])
    with pytest.raises(DecodeError):
        decode_delete(raw, relations)


def test_decode_message_dispatches_update_and_delete():
    decoder = Decoder()
    decoder.decode(_relation_bytes(7, "public", "widgets"))

    update_raw = _update_bytes(relation_id=7, old_marker=None, new=[("id", "5")])
    update = decoder.decode(update_raw)
    assert isinstance(update, Update)
    assert update.table == "public.widgets"

    delete_raw = _delete_bytes(7, b"K", [("id", "5")])
    delete = decoder.decode(delete_raw)
    assert isinstance(delete, Delete)
    assert delete.table == "public.widgets"
