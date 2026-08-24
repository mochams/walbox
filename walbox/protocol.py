"""Decode/encode functions for the inner PostgreSQL logical replication messages.

Covers `XLogData`, `PrimaryKeepaliveMessage`, and `StandbyStatusUpdate`: the
three inner message shapes that ride inside PostgreSQL's COPY BOTH stream.
The outer CopyData envelope is stripped and constructed by libpq itself
(`ReplicationTransport`), so every function here takes or returns a bare
inner-message payload. Pure functions and immutable value objects only,
no sockets or libpq.
"""

import logging
from dataclasses import dataclass
from datetime import UTC
from datetime import datetime

from walbox.errors import ErrorContext
from walbox.errors import ProtocolError

logger = logging.getLogger("walbox.protocol")

_PG_EPOCH = datetime(2000, 1, 1, tzinfo=UTC)

_XLOG_DATA_HEADER_LEN = 25  # Byte1 + 3 Int64 fields, before the payload
_PRIMARY_KEEPALIVE_LEN = 18  # Byte1 + 2 Int64 fields + Byte1 reply_requested


@dataclass(frozen=True, slots=True)
class XLogData:
    """A chunk of WAL data forwarded by the server, with its opaque payload."""

    wal_start: int
    wal_end: int
    send_time: int
    payload: bytes


@dataclass(frozen=True, slots=True)
class PrimaryKeepalive:
    """A liveness ping from the server, optionally requesting an immediate reply."""

    wal_end: int
    send_time: int
    reply_requested: bool


@dataclass(frozen=True, slots=True)
class StandbyStatusUpdate:
    """A client-to-server progress report of received/flushed/applied LSNs.

    Stores actual, un-adjusted LSNs. The `+1` PostgreSQL's wire format
    requires is applied only at encoding time, in
    `encode_standby_status_update`.
    """

    written_lsn: int
    flushed_lsn: int
    applied_lsn: int
    client_time: int
    reply_requested: bool


ReplicationMessage = XLogData | PrimaryKeepalive


def decode_xlog_data(payload: bytes) -> XLogData:
    """Decode a bare `XLogData` ('w') payload.

    Returns:
        The decoded `XLogData`.

    Raises:
        ProtocolError: If `payload` is too short or has the wrong leading byte.
    """
    if len(payload) < _XLOG_DATA_HEADER_LEN or payload[0:1] != b"w":
        message = "not a valid XLogData message"
        logger.error(message, extra={"message_type": "XLogData"})
        raise ProtocolError(message, context=ErrorContext(message_type="XLogData"))
    return XLogData(
        wal_start=int.from_bytes(payload[1:9], "big"),
        wal_end=int.from_bytes(payload[9:17], "big"),
        send_time=int.from_bytes(payload[17:25], "big"),
        payload=payload[25:],
    )


def decode_primary_keepalive(payload: bytes) -> PrimaryKeepalive:
    """Decode a bare `PrimaryKeepaliveMessage` ('k') payload.

    Returns:
        The decoded `PrimaryKeepalive`.

    Raises:
        ProtocolError: If `payload` isn't exactly 18 bytes or has the wrong
            leading byte.
    """
    if len(payload) != _PRIMARY_KEEPALIVE_LEN or payload[0:1] != b"k":
        message = "not a valid PrimaryKeepaliveMessage"
        logger.error(message, extra={"message_type": "PrimaryKeepalive"})
        raise ProtocolError(
            message,
            context=ErrorContext(message_type="PrimaryKeepalive"),
        )
    return PrimaryKeepalive(
        wal_end=int.from_bytes(payload[1:9], "big"),
        send_time=int.from_bytes(payload[9:17], "big"),
        reply_requested=payload[17] != 0,
    )


def decode_replication_message(payload: bytes) -> ReplicationMessage:
    """Dispatch a bare inner-message payload to its decoder by leading byte.

    Returns:
        The decoded `XLogData` or `PrimaryKeepalive`.

    Raises:
        ProtocolError: If `payload` is empty or its leading byte is unknown.
    """
    if not payload:
        message = "empty replication message payload"
        logger.error(message)
        raise ProtocolError(message)
    match payload[0:1]:
        case b"w":
            return decode_xlog_data(payload)
        case b"k":
            return decode_primary_keepalive(payload)
        case other:
            message = f"unknown replication message type {other!r}"
            logger.error(
                message,
                extra={"message_type": other.decode(errors="replace")},
            )
            raise ProtocolError(message)


def encode_standby_status_update(update: StandbyStatusUpdate) -> bytes:
    """Encode a `StandbyStatusUpdate` ('r') as a bare inner-message payload.

    Applies the `+1` PostgreSQL requires on each LSN field; `update`'s own
    fields stay the actual, un-adjusted positions.

    Returns:
        The bare 34-byte payload. The caller hands it to
        `pgconn.put_copy_data()`, which constructs the outer CopyData
        envelope itself.
    """
    return (
        b"r"
        + (update.written_lsn + 1).to_bytes(8, "big")
        + (update.flushed_lsn + 1).to_bytes(8, "big")
        + (update.applied_lsn + 1).to_bytes(8, "big")
        + update.client_time.to_bytes(8, "big")
        + bytes([1 if update.reply_requested else 0])
    )


def pg_now_micros(now: datetime | None = None) -> int:
    """Convert a UTC timestamp to microseconds since the PostgreSQL epoch.

    Returns:
        Microseconds elapsed since 2000-01-01 00:00:00 UTC.
    """
    moment = now if now is not None else datetime.now(UTC)
    return int((moment - _PG_EPOCH).total_seconds() * 1_000_000)
