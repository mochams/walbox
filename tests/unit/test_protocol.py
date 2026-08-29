from datetime import UTC
from datetime import datetime

import pytest

from walbox.errors import ProtocolError
from walbox.protocol import PrimaryKeepalive
from walbox.protocol import StandbyStatusUpdate
from walbox.protocol import XLogData
from walbox.protocol import decode_primary_keepalive
from walbox.protocol import decode_replication_message
from walbox.protocol import decode_xlog_data
from walbox.protocol import encode_standby_status_update
from walbox.protocol import pg_now_micros


def _xlog_data_bytes(
    wal_start: int,
    wal_end: int,
    send_time: int,
    payload: bytes,
) -> bytes:
    return (
        b"w"
        + wal_start.to_bytes(8, "big")
        + wal_end.to_bytes(8, "big")
        + send_time.to_bytes(8, "big")
        + payload
    )


def _keepalive_bytes(wal_end: int, send_time: int, reply_requested: bool) -> bytes:
    return (
        b"k"
        + wal_end.to_bytes(8, "big")
        + send_time.to_bytes(8, "big")
        + bytes([1 if reply_requested else 0])
    )


def test_decode_xlog_data_parses_all_three_lsn_like_fields_and_payload():
    raw = _xlog_data_bytes(
        wal_start=111,
        wal_end=222,
        send_time=333,
        payload=b"pgoutput-bytes",
    )
    decoded = decode_xlog_data(raw)
    assert decoded == XLogData(
        wal_start=111,
        wal_end=222,
        send_time=333,
        payload=b"pgoutput-bytes",
    )


def test_decode_xlog_data_rejects_a_too_short_payload():
    with pytest.raises(ProtocolError):
        decode_xlog_data(b"w" + b"\x00" * 23)


@pytest.mark.parametrize("reply_requested", [True, False])
def test_decode_primary_keepalive_parses_reply_requested_true_and_false(
    reply_requested,
):
    raw = _keepalive_bytes(wal_end=42, send_time=99, reply_requested=reply_requested)
    decoded = decode_primary_keepalive(raw)
    assert decoded == PrimaryKeepalive(
        wal_end=42,
        send_time=99,
        reply_requested=reply_requested,
    )


@pytest.mark.parametrize("length", [17, 19])
def test_decode_primary_keepalive_rejects_wrong_length(length):
    with pytest.raises(ProtocolError):
        decode_primary_keepalive(b"k" + b"\x00" * (length - 1))


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (
            _xlog_data_bytes(wal_start=1, wal_end=2, send_time=3, payload=b""),
            XLogData,
        ),
        (
            _keepalive_bytes(wal_end=1, send_time=2, reply_requested=False),
            PrimaryKeepalive,
        ),
        (b"x", ProtocolError),
    ],
)
def test_decode_replication_message_dispatches_on_leading_byte(raw, expected):
    if expected is ProtocolError:
        with pytest.raises(ProtocolError):
            decode_replication_message(raw)
    else:
        assert isinstance(decode_replication_message(raw), expected)


def test_decode_replication_message_rejects_empty_payload():
    with pytest.raises(ProtocolError):
        decode_replication_message(b"")


def test_encode_standby_status_update_applies_plus_one_to_all_three_lsns():
    update = StandbyStatusUpdate(
        written_lsn=99,
        flushed_lsn=50,
        applied_lsn=50,
        client_time=0,
        reply_requested=False,
    )
    encoded = encode_standby_status_update(update)
    written = int.from_bytes(encoded[1:9], "big")
    flushed = int.from_bytes(encoded[9:17], "big")
    applied = int.from_bytes(encoded[17:25], "big")
    assert (written, flushed, applied) == (100, 51, 51)


def test_encode_standby_status_update_returns_a_bare_payload_not_a_copy_data_frame():
    update = StandbyStatusUpdate(
        written_lsn=1,
        flushed_lsn=1,
        applied_lsn=1,
        client_time=0,
        reply_requested=False,
    )
    encoded = encode_standby_status_update(update)
    assert encoded[0:1] == b"r"
    assert len(encoded) == 34


@pytest.mark.parametrize("reply_requested", [True, False])
def test_encode_standby_status_update_reply_requested_flag_round_trips(
    reply_requested,
):
    update = StandbyStatusUpdate(
        written_lsn=1,
        flushed_lsn=1,
        applied_lsn=1,
        client_time=0,
        reply_requested=reply_requested,
    )
    encoded = encode_standby_status_update(update)
    assert encoded[-1] == (1 if reply_requested else 0)


def test_pg_now_micros_at_exact_epoch_is_zero():
    assert pg_now_micros(datetime(2000, 1, 1, tzinfo=UTC)) == 0


def test_pg_now_micros_one_second_after_epoch():
    assert pg_now_micros(datetime(2000, 1, 1, 0, 0, 1, tzinfo=UTC)) == 1_000_000
