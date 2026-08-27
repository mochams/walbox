import uuid

import pytest
from psycopg import AsyncConnection
from psycopg import OperationalError

from walbox.errors import ReplicationConnectionError
from walbox.protocol import decode_xlog_data
from walbox.transport import ReplicationTransport

pytestmark = pytest.mark.postgres

_INSERT_OUTBOX_ROW = (
    "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
    "VALUES ('user', 'user-1', 'user_created', '{\"a\": 1}'::jsonb)"
)


def _unique_slot_name() -> str:
    return f"slot_{uuid.uuid4().hex}"


async def _read_xlog_data(transport: ReplicationTransport, attempts: int = 20) -> bytes:
    """Read until an XLogData ('w') payload appears, skipping keepalives ('k').

    transport.read() intentionally returns every message unfiltered, so a
    keepalive can legitimately arrive before the row-change message a test
    is actually waiting for.
    """
    for _ in range(attempts):
        payload = await transport.read()
        if payload[0:1] == b"w":
            return payload
    pytest.fail(f"did not receive an XLogData message within {attempts} reads")


async def test_connect_establishes_a_working_replication_connection(postgres_dsn):
    transport = ReplicationTransport(postgres_dsn, _unique_slot_name(), "walbox_pub")
    await transport.connect()
    try:
        assert transport._conn.pgconn.socket >= 0
    finally:
        transport.close()


async def test_connect_raises_replication_connection_error_for_an_unreachable_host():
    transport = ReplicationTransport(
        "host=127.0.0.1 port=1 dbname=x user=x password=x connect_timeout=2",
        _unique_slot_name(),
        "walbox_pub",
    )
    with pytest.raises(ReplicationConnectionError):
        await transport.connect()


async def test_create_slot_if_missing_creates_a_new_slot(postgres_dsn):
    slot_name = _unique_slot_name()
    transport = ReplicationTransport(postgres_dsn, slot_name, "walbox_pub")
    await transport.connect()
    try:
        await transport.create_slot_if_missing()
        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT plugin FROM pg_replication_slots WHERE slot_name = %s",
                    (slot_name,),
                )
                row = await cur.fetchone()
        assert row is not None
        assert row[0] == "pgoutput"
    finally:
        transport.close()


async def test_create_slot_if_missing_is_idempotent(postgres_dsn):
    slot_name = _unique_slot_name()
    transport = ReplicationTransport(postgres_dsn, slot_name, "walbox_pub")
    await transport.connect()
    try:
        await transport.create_slot_if_missing()
        await transport.create_slot_if_missing()
    finally:
        transport.close()


async def test_create_slot_if_missing_raises_for_non_duplicate_failures(postgres_dsn):
    transport = ReplicationTransport(
        postgres_dsn,
        "not a valid slot name!",
        "walbox_pub",
    )
    await transport.connect()
    try:
        with pytest.raises(ReplicationConnectionError):
            await transport.create_slot_if_missing()
    finally:
        transport.close()


async def test_start_replication_enters_copy_both_and_delivers_bytes_after_an_insert(
    postgres_dsn,
    outbox_table,
):
    slot_name = _unique_slot_name()
    transport = ReplicationTransport(postgres_dsn, slot_name, "walbox_pub")
    await transport.connect()
    try:
        await transport.create_slot_if_missing()
        await transport.start_replication(start_lsn=0)

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(_INSERT_OUTBOX_ROW)

        payload = await _read_xlog_data(transport)
        assert payload[0:1] == b"w"
    finally:
        transport.close()


async def test_start_replication_raises_for_a_nonexistent_slot(postgres_dsn):
    transport = ReplicationTransport(postgres_dsn, _unique_slot_name(), "walbox_pub")
    await transport.connect()
    try:
        with pytest.raises(ReplicationConnectionError):
            await transport.start_replication(start_lsn=0)
    finally:
        transport.close()


async def test_read_raises_replication_connection_error_when_server_closes_the_connection(
    postgres_dsn,
    outbox_table,
):
    slot_name = _unique_slot_name()
    transport = ReplicationTransport(postgres_dsn, slot_name, "walbox_pub")
    await transport.connect()
    try:
        await transport.create_slot_if_missing()
        await transport.start_replication(start_lsn=0)
        backend_pid = transport._conn.pgconn.backend_pid

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute("SELECT pg_terminate_backend(%s)", (backend_pid,))

        with pytest.raises(ReplicationConnectionError):
            for _ in range(20):
                await transport.read()
    finally:
        transport.close()


async def test_write_sends_bytes_without_error(postgres_dsn, outbox_table):
    slot_name = _unique_slot_name()
    transport = ReplicationTransport(postgres_dsn, slot_name, "walbox_pub")
    await transport.connect()
    try:
        await transport.create_slot_if_missing()
        await transport.start_replication(start_lsn=0)

        payload = (
            b"r"
            + (0).to_bytes(8, "big")
            + (0).to_bytes(8, "big")
            + (0).to_bytes(8, "big")
            + (0).to_bytes(8, "big")
            + bytes([0])
        )
        await transport.write(payload)
    finally:
        transport.close()


async def test_end_copy_sends_copy_done_without_error(postgres_dsn, outbox_table):
    slot_name = _unique_slot_name()
    transport = ReplicationTransport(postgres_dsn, slot_name, "walbox_pub")
    await transport.connect()
    try:
        await transport.create_slot_if_missing()
        await transport.start_replication(start_lsn=0)
        await transport.end_copy()
    finally:
        transport.close()


async def test_end_copy_raises_replication_connection_error_when_server_closes_the_connection(
    postgres_dsn,
    outbox_table,
):
    slot_name = _unique_slot_name()
    transport = ReplicationTransport(postgres_dsn, slot_name, "walbox_pub")
    await transport.connect()
    try:
        await transport.create_slot_if_missing()
        await transport.start_replication(start_lsn=0)
        backend_pid = transport._conn.pgconn.backend_pid

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute("SELECT pg_terminate_backend(%s)", (backend_pid,))

        with pytest.raises(ReplicationConnectionError):
            for _ in range(20):
                await transport.end_copy()
    finally:
        transport.close()


async def test_close_is_safe_to_call_twice(postgres_dsn):
    transport = ReplicationTransport(postgres_dsn, _unique_slot_name(), "walbox_pub")
    await transport.connect()
    transport.close()
    transport.close()


async def test_replication_works_over_a_tls_connection(
    tls_postgres_dsn,
    tls_outbox_table,
):
    slot_name = _unique_slot_name()
    transport = ReplicationTransport(tls_postgres_dsn, slot_name, "walbox_pub")
    await transport.connect()
    try:
        await transport.create_slot_if_missing()
        await transport.start_replication(start_lsn=0)

        async with await AsyncConnection.connect(
            tls_postgres_dsn,
            autocommit=True,
        ) as conn:
            await conn.execute(_INSERT_OUTBOX_ROW)

        payload = await _read_xlog_data(transport)
        assert decode_xlog_data(payload).payload != b""
    finally:
        transport.close()


async def test_tls_postgres_container_rejects_non_ssl_connections(
    tls_postgres_container,
):
    """The hostssl pg_hba.conf rewrite actually rejects plaintext, proving
    the TLS container requires TLS rather than merely offering it.
    """
    base = tls_postgres_container.get_connection_url(driver=None)
    separator = "&" if "?" in base else "?"
    plaintext_dsn = f"{base}{separator}sslmode=disable"
    with pytest.raises(OperationalError):
        async with await AsyncConnection.connect(plaintext_dsn, autocommit=True):
            pass
