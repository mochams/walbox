"""Unit tests for ReplicationTransport's retry/race and error-handling paths.

These exercise branches a real Postgres connection can't reliably force
(would-block retries, specific OperationalError failures) by mocking
`pgconn` directly -- no Docker needed. `_wait_readable`/`_wait_writable`
still run for real against a real `socket.socketpair()` fd, since they're
genuine asyncio readiness plumbing worth exercising as written; only the
libpq-facing `pgconn` calls themselves are mocked.
"""

import socket
from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from psycopg import OperationalError
from psycopg.pq import ExecStatus

from walbox.errors import ReplicationConnectionError
from walbox.transport import AsyncConnection
from walbox.transport import ReplicationTransport


def _transport_with_mock_pgconn(pgconn: MagicMock) -> ReplicationTransport:
    transport = ReplicationTransport("dsn", "test_slot", "test_pub")
    transport._conn = SimpleNamespace(pgconn=pgconn)
    return transport


@pytest.fixture
def socket_pair():
    sock, peer = socket.socketpair()
    sock.setblocking(False)
    peer.setblocking(False)
    yield sock, peer
    sock.close()
    peer.close()


def _make_unwritable(sock: socket.socket) -> None:
    """Fill sock's own send buffer so it's genuinely not write-ready."""
    try:
        while True:
            sock.send(b"\x00" * 65536)
    except BlockingIOError:
        return


async def test_flush_until_complete_retries_on_would_block_and_cancels_the_losing_wait(
    socket_pair,
):
    sock, _peer = socket_pair
    pgconn = MagicMock()
    pgconn.socket = sock.fileno()
    pgconn.flush.side_effect = [1, 0]

    transport = _transport_with_mock_pgconn(pgconn)
    await transport._flush_until_complete()

    assert pgconn.flush.call_count == 2
    pgconn.consume_input.assert_not_called()


async def test_flush_until_complete_consumes_input_when_the_socket_becomes_readable(
    socket_pair,
):
    sock, peer = socket_pair
    _make_unwritable(sock)
    peer.send(b"x")

    pgconn = MagicMock()
    pgconn.socket = sock.fileno()
    pgconn.flush.side_effect = [1, 0]

    transport = _transport_with_mock_pgconn(pgconn)
    await transport._flush_until_complete()

    pgconn.consume_input.assert_called_once()
    assert pgconn.flush.call_count == 2


async def test_flush_raises_replication_connection_error_when_flush_fails():
    pgconn = MagicMock()
    pgconn.flush.side_effect = OperationalError("boom")

    transport = _transport_with_mock_pgconn(pgconn)
    with pytest.raises(ReplicationConnectionError):
        await transport._flush()


async def test_write_retries_via_wait_writable_when_put_copy_data_would_block(
    socket_pair,
):
    sock, _peer = socket_pair
    pgconn = MagicMock()
    pgconn.socket = sock.fileno()
    pgconn.put_copy_data.side_effect = [0, 1]
    pgconn.flush.return_value = 0

    transport = _transport_with_mock_pgconn(pgconn)
    await transport.write(b"payload")

    assert pgconn.put_copy_data.call_count == 2
    pgconn.flush.assert_called_once()


async def test_write_raises_replication_connection_error_when_put_copy_data_fails():
    pgconn = MagicMock()
    pgconn.put_copy_data.side_effect = OperationalError("boom")

    transport = _transport_with_mock_pgconn(pgconn)
    with pytest.raises(ReplicationConnectionError):
        await transport.write(b"payload")


async def test_end_copy_retries_via_wait_writable_when_put_copy_end_would_block(
    socket_pair,
):
    sock, _peer = socket_pair
    pgconn = MagicMock()
    pgconn.socket = sock.fileno()
    pgconn.put_copy_end.side_effect = [0, 1]
    pgconn.flush.return_value = 0

    transport = _transport_with_mock_pgconn(pgconn)
    await transport.end_copy()

    assert pgconn.put_copy_end.call_count == 2
    pgconn.flush.assert_called_once()


async def test_read_raises_replication_connection_error_when_get_copy_data_fails():
    pgconn = MagicMock()
    pgconn.get_copy_data.side_effect = OperationalError("boom")

    transport = _transport_with_mock_pgconn(pgconn)
    with pytest.raises(ReplicationConnectionError):
        await transport.read()


async def test_start_replication_negotiates_serial_streaming(socket_pair):
    """Every connection opts into streaming, unconditionally.

    `proto_version '2'` and `streaming 'on'` must both be present -- a
    transaction that never grows large enough to stream is unaffected, but
    the options string itself is not conditional on any workload.
    """
    sock, _peer = socket_pair
    pgconn = MagicMock()
    pgconn.socket = sock.fileno()
    pgconn.flush.return_value = 0
    pgconn.is_busy.return_value = False
    copy_both_result = MagicMock(status=ExecStatus.COPY_BOTH)
    pgconn.get_result.side_effect = [copy_both_result, None]

    transport = _transport_with_mock_pgconn(pgconn)
    await transport.start_replication(0)

    sent_command = pgconn.send_query.call_args[0][0].decode()
    assert "proto_version '2'" in sent_command
    assert "streaming 'on'" in sent_command
    assert "publication_names 'test_pub'" in sent_command


async def test_start_replication_raises_when_not_copy_both(socket_pair):
    sock, _peer = socket_pair
    pgconn = MagicMock()
    pgconn.socket = sock.fileno()
    pgconn.flush.return_value = 0
    pgconn.is_busy.return_value = False
    bad_result = MagicMock(status=ExecStatus.FATAL_ERROR)
    pgconn.get_result.side_effect = [bad_result, None]
    pgconn.error_message = b"replication failed"

    transport = _transport_with_mock_pgconn(pgconn)
    with pytest.raises(ReplicationConnectionError):
        await transport.start_replication(0)


async def test_connect_sets_the_connection_on_success(monkeypatch):
    fake_conn = SimpleNamespace(pgconn=MagicMock())
    connect_mock = AsyncMock(return_value=fake_conn)
    monkeypatch.setattr(AsyncConnection, "connect", connect_mock)

    transport = ReplicationTransport("postgresql://example", "test_slot", "test_pub")
    await transport.connect()

    assert transport._conn is fake_conn
    connect_mock.assert_awaited_once()
    _args, kwargs = connect_mock.await_args
    assert kwargs["autocommit"] is True


async def test_connect_wraps_operational_error(monkeypatch):
    connect_mock = AsyncMock(side_effect=OperationalError("boom"))
    monkeypatch.setattr(AsyncConnection, "connect", connect_mock)

    transport = ReplicationTransport("postgresql://example", "test_slot", "test_pub")
    with pytest.raises(ReplicationConnectionError):
        await transport.connect()


async def test_drain_result_waits_while_the_connection_is_busy(socket_pair):
    sock, peer = socket_pair
    peer.send(b"x")
    pgconn = MagicMock()
    pgconn.socket = sock.fileno()
    pgconn.is_busy.side_effect = [True, False]
    ok_result = MagicMock(status=ExecStatus.COMMAND_OK)
    pgconn.get_result.side_effect = [ok_result, None]

    transport = _transport_with_mock_pgconn(pgconn)
    result = await transport._drain_result()

    assert result is ok_result
    pgconn.consume_input.assert_called_once()


async def test_drain_result_continues_past_non_copy_statuses():
    pgconn = MagicMock()
    pgconn.is_busy.return_value = False
    first_result = MagicMock(status=ExecStatus.COMMAND_OK)
    pgconn.get_result.side_effect = [first_result, None]

    transport = _transport_with_mock_pgconn(pgconn)
    result = await transport._drain_result()

    assert result is first_result
    assert pgconn.get_result.call_count == 2


async def test_create_slot_if_missing_succeeds(socket_pair):
    sock, _peer = socket_pair
    pgconn = MagicMock()
    pgconn.socket = sock.fileno()
    pgconn.flush.return_value = 0
    pgconn.is_busy.return_value = False
    ok_result = MagicMock(status=ExecStatus.COMMAND_OK)
    pgconn.get_result.side_effect = [ok_result, None]

    transport = _transport_with_mock_pgconn(pgconn)
    await transport.create_slot_if_missing()

    sent_command = pgconn.send_query.call_args[0][0].decode()
    assert "CREATE_REPLICATION_SLOT test_slot LOGICAL pgoutput" in sent_command


async def test_create_slot_if_missing_tolerates_the_slot_already_existing(
    socket_pair,
):
    sock, _peer = socket_pair
    pgconn = MagicMock()
    pgconn.socket = sock.fileno()
    pgconn.flush.return_value = 0
    pgconn.is_busy.return_value = False
    fatal_result = MagicMock(status=ExecStatus.FATAL_ERROR)
    fatal_result.error_field.return_value = b"42710"
    pgconn.get_result.side_effect = [fatal_result, None]

    transport = _transport_with_mock_pgconn(pgconn)
    await transport.create_slot_if_missing()  # must not raise


async def test_create_slot_if_missing_raises_on_other_fatal_errors(socket_pair):
    sock, _peer = socket_pair
    pgconn = MagicMock()
    pgconn.socket = sock.fileno()
    pgconn.flush.return_value = 0
    pgconn.is_busy.return_value = False
    fatal_result = MagicMock(status=ExecStatus.FATAL_ERROR)
    fatal_result.error_field.return_value = b"XXXXX"
    pgconn.get_result.side_effect = [fatal_result, None]
    pgconn.error_message = b"some other error"

    transport = _transport_with_mock_pgconn(pgconn)
    with pytest.raises(ReplicationConnectionError):
        await transport.create_slot_if_missing()


async def test_read_until_complete_retries_when_no_data_is_ready_yet(socket_pair):
    sock, peer = socket_pair
    peer.send(b"x")
    pgconn = MagicMock()
    pgconn.socket = sock.fileno()
    pgconn.get_copy_data.side_effect = [(0, b""), (3, b"abc")]

    transport = _transport_with_mock_pgconn(pgconn)
    result = await transport._read_until_complete()

    assert result == b"abc"
    pgconn.consume_input.assert_called_once()


async def test_read_until_complete_raises_when_the_stream_ends():
    pgconn = MagicMock()
    pgconn.get_copy_data.return_value = (-1, b"")

    transport = _transport_with_mock_pgconn(pgconn)
    with pytest.raises(ReplicationConnectionError, match="stream ended"):
        await transport._read_until_complete()


async def test_end_copy_raises_replication_connection_error_when_put_copy_end_fails():
    pgconn = MagicMock()
    pgconn.put_copy_end.side_effect = OperationalError("boom")

    transport = _transport_with_mock_pgconn(pgconn)
    with pytest.raises(ReplicationConnectionError):
        await transport.end_copy()


def test_close_finishes_the_connection_and_clears_it():
    pgconn = MagicMock()
    transport = _transport_with_mock_pgconn(pgconn)

    transport.close()

    pgconn.finish.assert_called_once()
    assert transport._conn is None


def test_close_is_a_no_op_when_never_connected():
    transport = ReplicationTransport("dsn", "test_slot", "test_pub")

    transport.close()  # must not raise

    assert transport._conn is None
