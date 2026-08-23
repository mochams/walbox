"""Unit tests for streamed-transaction pgoutput decoding.

Covers the four streaming message kinds (`StreamStart`/`StreamStop`/
`StreamCommit`/`StreamAbort`), the leading-xid handling that
`decode_insert`/`decode_update`/`decode_delete`/`decode_truncate`/
`decode_relation`/`decode_type` gain while a streaming bracket is open, and
`Decoder`'s bracket-tracking dispatch.
"""

import pytest

from walbox.errors import DecodeError
from walbox.pgoutput import Column
from walbox.pgoutput import Decoder
from walbox.pgoutput import Delete
from walbox.pgoutput import Insert
from walbox.pgoutput import Relation
from walbox.pgoutput import RelationCache
from walbox.pgoutput import StreamAbort
from walbox.pgoutput import StreamCommit
from walbox.pgoutput import StreamStart
from walbox.pgoutput import StreamStop
from walbox.pgoutput import Truncate
from walbox.pgoutput import Type
from walbox.pgoutput import Update
from walbox.pgoutput import decode_delete
from walbox.pgoutput import decode_insert
from walbox.pgoutput import decode_relation
from walbox.pgoutput import decode_stream_abort
from walbox.pgoutput import decode_stream_commit
from walbox.pgoutput import decode_stream_start
from walbox.pgoutput import decode_stream_stop
from walbox.pgoutput import decode_truncate
from walbox.pgoutput import decode_type
from walbox.pgoutput import decode_update


def _cstring(value: str) -> bytes:
    return value.encode("utf-8") + b"\x00"


def _things_relation(relation_id: int = 1) -> Relation:
    return Relation(
        relation_id=relation_id,
        namespace="public",
        name="things",
        replica_identity="d",
        columns=(Column(name="id", type_oid=23, type_modifier=-1, is_key=True),),
    )


def _relation_bytes(
    relation_id: int,
    namespace: str,
    name: str,
    *,
    xid: int | None = None,
) -> bytes:
    columns = [("id", 23, -1, True)]
    body = b"R"
    if xid is not None:
        body += xid.to_bytes(4, "big")
    body += (
        relation_id.to_bytes(4, "big")
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


def _insert_bytes(relation_id: int, *, xid: int | None = None) -> bytes:
    body = b"I"
    if xid is not None:
        body += xid.to_bytes(4, "big")
    return (
        body
        + relation_id.to_bytes(4, "big")
        + b"N"
        + (1).to_bytes(2, "big")
        + b"t"
        + (1).to_bytes(4, "big")
        + b"5"
    )


def _update_bytes(relation_id: int, *, xid: int | None = None) -> bytes:
    body = b"U"
    if xid is not None:
        body += xid.to_bytes(4, "big")
    return (
        body
        + relation_id.to_bytes(4, "big")
        + b"N"
        + (1).to_bytes(2, "big")
        + b"t"
        + (1).to_bytes(4, "big")
        + b"5"
    )


def _delete_bytes(relation_id: int, *, xid: int | None = None) -> bytes:
    body = b"D"
    if xid is not None:
        body += xid.to_bytes(4, "big")
    return (
        body
        + relation_id.to_bytes(4, "big")
        + b"K"
        + (1).to_bytes(2, "big")
        + b"t"
        + (1).to_bytes(4, "big")
        + b"5"
    )


def _truncate_bytes(
    relation_ids: list[int],
    *,
    flags: int = 0,
    xid: int | None = None,
) -> bytes:
    body = b"T"
    if xid is not None:
        body += xid.to_bytes(4, "big")
    body += len(relation_ids).to_bytes(4, "big") + bytes([flags])
    for relation_id in relation_ids:
        body += relation_id.to_bytes(4, "big")
    return body


def _type_bytes(
    type_oid: int,
    namespace: str,
    name: str,
    *,
    xid: int | None = None,
) -> bytes:
    body = b"Y"
    if xid is not None:
        body += xid.to_bytes(4, "big")
    return body + type_oid.to_bytes(4, "big") + _cstring(namespace) + _cstring(name)


def _stream_start_bytes(xid: int, *, first_segment: bool) -> bytes:
    return b"S" + xid.to_bytes(4, "big") + bytes([1 if first_segment else 0])


def _stream_stop_bytes() -> bytes:
    return b"E"


def _stream_commit_bytes(
    xid: int,
    *,
    commit_lsn: int,
    end_lsn: int,
    commit_time: int,
) -> bytes:
    return (
        b"c"
        + xid.to_bytes(4, "big")
        + bytes([0])
        + commit_lsn.to_bytes(8, "big")
        + end_lsn.to_bytes(8, "big")
        + commit_time.to_bytes(8, "big")
    )


def _stream_abort_bytes(xid: int, subxid: int) -> bytes:
    return b"A" + xid.to_bytes(4, "big") + subxid.to_bytes(4, "big")


# -- StreamStart --------------------------------------------------------


def test_decode_stream_start_parses_xid_and_first_segment_flag():
    decoded = decode_stream_start(_stream_start_bytes(42, first_segment=True))
    assert decoded == StreamStart(xid=42, first_segment=True)

    decoded = decode_stream_start(_stream_start_bytes(42, first_segment=False))
    assert decoded == StreamStart(xid=42, first_segment=False)


def test_decode_stream_start_rejects_wrong_leading_byte():
    with pytest.raises(DecodeError):
        decode_stream_start(b"X" + b"\x00" * 5)


def test_decode_stream_start_rejects_wrong_length():
    with pytest.raises(DecodeError):
        decode_stream_start(b"S" + b"\x00" * 3)


# -- StreamStop -----------------------------------------------------------


def test_decode_stream_stop_message():
    assert decode_stream_stop(_stream_stop_bytes()) == StreamStop()


def test_decode_stream_stop_rejects_wrong_leading_byte():
    with pytest.raises(DecodeError):
        decode_stream_stop(b"X")


def test_decode_stream_stop_rejects_wrong_length():
    with pytest.raises(DecodeError):
        decode_stream_stop(b"E" + b"\x00")


# -- StreamCommit -----------------------------------------------------------


def test_decode_stream_commit_parses_xid_and_all_three_lsn_like_fields():
    raw = _stream_commit_bytes(7, commit_lsn=100, end_lsn=200, commit_time=300)
    decoded = decode_stream_commit(raw)
    assert decoded == StreamCommit(xid=7, commit_lsn=100, end_lsn=200, commit_time=300)


def test_decode_stream_commit_rejects_wrong_leading_byte():
    with pytest.raises(DecodeError):
        decode_stream_commit(b"X" + b"\x00" * 29)


def test_decode_stream_commit_rejects_wrong_length():
    with pytest.raises(DecodeError):
        decode_stream_commit(b"c" + b"\x00" * 10)


# -- StreamAbort --------------------------------------------------------


def test_decode_stream_abort_parses_xid_and_subxid():
    decoded = decode_stream_abort(_stream_abort_bytes(7, 9))
    assert decoded == StreamAbort(xid=7, subxid=9)


def test_decode_stream_abort_full_transaction_has_equal_xid_and_subxid():
    decoded = decode_stream_abort(_stream_abort_bytes(7, 7))
    assert decoded.xid == decoded.subxid == 7


def test_decode_stream_abort_rejects_wrong_leading_byte():
    with pytest.raises(DecodeError):
        decode_stream_abort(b"X" + b"\x00" * 8)


def test_decode_stream_abort_rejects_wrong_length():
    with pytest.raises(DecodeError):
        decode_stream_abort(b"A" + b"\x00" * 4)


# -- streaming=True leading-xid handling on existing decoders ------------


def test_decode_insert_reads_leading_xid_only_when_streaming():
    relations = RelationCache()
    relations.add(_things_relation(1))

    non_streaming = decode_insert(_insert_bytes(1), relations, streaming=False)
    assert non_streaming.new == {"id": "5"}
    assert non_streaming.subxid is None

    streaming = decode_insert(
        _insert_bytes(1, xid=999),
        relations,
        streaming=True,
    )
    assert streaming.new == {"id": "5"}
    assert streaming.subxid == 999


def test_decode_insert_streaming_default_is_false():
    relations = RelationCache()
    relations.add(_things_relation(1))
    decoded = decode_insert(_insert_bytes(1), relations)
    assert decoded.new == {"id": "5"}


def test_decode_insert_misparses_without_streaming_flag_when_xid_present():
    relations = RelationCache()
    relations.add(_things_relation(1))
    with pytest.raises(DecodeError):
        decode_insert(_insert_bytes(1, xid=999), relations, streaming=False)


def test_decode_update_reads_leading_xid_when_streaming():
    relations = RelationCache()
    relations.add(_things_relation(7))

    update = decode_update(_update_bytes(7, xid=123), relations, streaming=True)
    assert isinstance(update, Update)
    assert update.new == {"id": "5"}
    assert update.subxid == 123

    non_streaming = decode_update(_update_bytes(7), relations, streaming=False)
    assert non_streaming.subxid is None


def test_decode_delete_reads_leading_xid_when_streaming():
    relations = RelationCache()
    relations.add(_things_relation(7))

    delete = decode_delete(_delete_bytes(7, xid=123), relations, streaming=True)
    assert isinstance(delete, Delete)
    assert delete.old == {"id": "5"}
    assert delete.subxid == 123

    non_streaming = decode_delete(_delete_bytes(7), relations, streaming=False)
    assert non_streaming.subxid is None


def test_decode_truncate_reads_leading_xid_once_before_relation_count():
    relations = RelationCache()
    relations.add(_things_relation(1))
    relations.add(Relation(2, "public", "others", "d", ()))

    truncate = decode_truncate(
        _truncate_bytes([1, 2], xid=123),
        relations,
        streaming=True,
    )
    assert isinstance(truncate, Truncate)
    assert truncate.tables == ("public.things", "public.others")
    assert truncate.subxid == 123

    non_streaming = decode_truncate(_truncate_bytes([1, 2]), relations)
    assert non_streaming.subxid is None


def test_decode_relation_skips_leading_xid_when_streaming():
    relation = decode_relation(
        _relation_bytes(7, "public", "widgets", xid=123),
        streaming=True,
    )
    assert relation.relation_id == 7
    assert relation.qualified_name == "public.widgets"


def test_decode_type_skips_leading_xid_when_streaming():
    decoded = decode_type(
        _type_bytes(16400, "public", "my_enum", xid=123),
        streaming=True,
    )
    assert decoded == Type(type_oid=16400, namespace="public", name="my_enum")


# -- Decoder bracket tracking --------------------------------------------


def test_decoder_dispatches_stream_start_and_stream_stop():
    decoder = Decoder()
    start = decoder.decode(_stream_start_bytes(1, first_segment=True))
    assert start == StreamStart(xid=1, first_segment=True)

    stop = decoder.decode(_stream_stop_bytes())
    assert stop == StreamStop()


def test_decoder_reads_leading_xid_for_row_messages_inside_open_bracket():
    decoder = Decoder()
    decoder.decode(_relation_bytes(7, "public", "widgets"))
    decoder.decode(_stream_start_bytes(42, first_segment=True))

    insert = decoder.decode(_insert_bytes(7, xid=42))
    assert isinstance(insert, Insert)
    assert insert.table == "public.widgets"
    assert insert.new == {"id": "5"}


def test_decoder_stops_reading_leading_xid_after_stream_stop():
    decoder = Decoder()
    decoder.decode(_relation_bytes(7, "public", "widgets"))
    decoder.decode(_stream_start_bytes(42, first_segment=True))
    decoder.decode(_insert_bytes(7, xid=42))
    decoder.decode(_stream_stop_bytes())

    insert = decoder.decode(_insert_bytes(7))
    assert isinstance(insert, Insert)
    assert insert.new == {"id": "5"}


def test_decoder_handles_relation_message_inside_open_streaming_bracket():
    decoder = Decoder()
    decoder.decode(_stream_start_bytes(42, first_segment=True))

    relation = decoder.decode(_relation_bytes(7, "public", "widgets", xid=42))
    assert isinstance(relation, Relation)
    assert relation.qualified_name == "public.widgets"

    insert = decoder.decode(_insert_bytes(7, xid=42))
    assert insert.table == "public.widgets"


def test_decoder_handles_type_message_inside_open_streaming_bracket():
    decoder = Decoder()
    decoder.decode(_relation_bytes(7, "public", "widgets"))
    decoder.decode(_stream_start_bytes(42, first_segment=True))

    decoded_type = decoder.decode(_type_bytes(16400, "public", "my_enum", xid=42))
    assert isinstance(decoded_type, Type)

    insert = decoder.decode(_insert_bytes(7, xid=42))
    assert isinstance(insert, Insert)
    assert insert.new == {"id": "5"}


def test_decoder_dispatches_stream_commit_and_stream_abort():
    decoder = Decoder()
    commit = decoder.decode(
        _stream_commit_bytes(42, commit_lsn=100, end_lsn=200, commit_time=300),
    )
    assert commit == StreamCommit(xid=42, commit_lsn=100, end_lsn=200, commit_time=300)

    abort = decoder.decode(_stream_abort_bytes(7, 7))
    assert abort == StreamAbort(xid=7, subxid=7)
