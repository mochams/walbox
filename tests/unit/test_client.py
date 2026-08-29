from dataclasses import dataclass
from unittest.mock import AsyncMock

from walbox.abc import WalboxOptions
from walbox.client import Client
from walbox.protocol import PrimaryKeepalive
from walbox.protocol import XLogData
from walbox.transport import ReplicationTransport


@dataclass
class _FakeCheckpointStore:
    """A minimal `CheckpointStore` stand-in, no disk/DB involved."""

    checkpoint_lsn: int | None

    async def load(self) -> int | None:
        return self.checkpoint_lsn

    async def save(self, lsn: int, *, connection: object | None = None) -> None:
        raise NotImplementedError


def _options() -> WalboxOptions:
    return WalboxOptions(
        consumer_name="test-consumer",
        dsn="postgresql://example",
        slot_name="test_slot",
        publication_name="test_pub",
    )


def _client(checkpoint_lsn: int | None = None) -> Client:
    return Client(_options(), _FakeCheckpointStore(checkpoint_lsn))


def _begin_payload(final_lsn: int = 100, commit_time: int = 111, xid: int = 1) -> bytes:
    return (
        b"B"
        + final_lsn.to_bytes(8, "big")
        + commit_time.to_bytes(8, "big")
        + xid.to_bytes(4, "big")
    )


def _type_payload(
    type_oid: int = 16400,
    namespace: str = "public",
    name: str = "e",
) -> bytes:
    def _cstring(value: str) -> bytes:
        return value.encode("utf-8") + b"\x00"

    return b"Y" + type_oid.to_bytes(4, "big") + _cstring(namespace) + _cstring(name)


def _origin_payload(origin_lsn: int = 1, name: str = "my_origin") -> bytes:
    return b"O" + origin_lsn.to_bytes(8, "big") + (name.encode("utf-8") + b"\x00")


def test_new_transport_uses_the_configured_dsn_slot_and_publication():
    options = WalboxOptions(
        consumer_name="test-consumer",
        dsn="postgresql://configured-dsn",
        slot_name="configured_slot",
        publication_name="configured_pub",
    )
    client = Client(options, _FakeCheckpointStore(None))

    transport = client._new_transport()

    assert isinstance(transport, ReplicationTransport)
    assert transport._dsn == "postgresql://configured-dsn"
    assert transport._slot_name == "configured_slot"
    assert transport._publication_name == "configured_pub"


async def test_no_checkpoint_starts_replication_from_zero_with_a_zero_floor():
    client = _client(checkpoint_lsn=None)
    client._transport = AsyncMock()

    checkpoint_lsn = await client.checkpoint_store.load()
    assert checkpoint_lsn is None

    await _run_until_start_replication(client)

    client._transport.start_replication.assert_awaited_once_with(0)
    assert client._durable_lsn == 0


async def test_existing_checkpoint_starts_replication_one_past_it_with_the_checkpoint_as_the_floor():
    client = _client(checkpoint_lsn=500)
    client._transport = AsyncMock()

    await _run_until_start_replication(client)

    client._transport.start_replication.assert_awaited_once_with(501)
    assert client._durable_lsn == 500


async def _run_until_start_replication(client: Client) -> None:
    """Drive just `run`'s startup logic, stopping before the read loop.

    `client._transport` is pre-set to a mock by the caller; `run` overwrites
    it via `_new_transport()`, so this patches `_new_transport` to return
    that same mock, then stops the loop by having `read()` raise. The
    receiver task raises inside `asyncio.TaskGroup`, which always surfaces
    child failures wrapped in an `ExceptionGroup`, not the bare exception.
    """
    transport = client._transport
    client._new_transport = lambda: transport
    transport.read.side_effect = StopAsyncIteration
    try:
        await client.run(AsyncMock())
    except ExceptionGroup:
        pass


async def test_handle_keepalive_ignores_reply_not_requested():
    client = _client()
    client._transport = AsyncMock()
    keepalive = PrimaryKeepalive(wal_end=100, send_time=0, reply_requested=False)

    await client._handle_keepalive(keepalive)

    client._transport.write.assert_not_awaited()


async def test_handle_keepalive_writes_a_status_update_when_reply_requested():
    client = _client()
    client._transport = AsyncMock()
    keepalive = PrimaryKeepalive(wal_end=100, send_time=0, reply_requested=True)

    await client._handle_keepalive(keepalive)

    client._transport.write.assert_awaited_once()
    (payload,), _kwargs = client._transport.write.await_args
    assert payload.startswith(b"r")


async def test_handle_keepalive_updates_last_written_lsn_from_wal_end_even_without_a_reply():
    client = _client()
    client._transport = AsyncMock()
    keepalive = PrimaryKeepalive(wal_end=777, send_time=0, reply_requested=False)

    await client._handle_keepalive(keepalive)

    assert client._last_written_lsn == 777


async def test_handle_xlog_data_updates_last_written_lsn_from_wal_start_not_wal_end():
    client = _client()
    xlog = XLogData(
        wal_start=100,
        wal_end=999,
        send_time=0,
        payload=_begin_payload(),
    )

    await client._handle_xlog_data(xlog)

    assert client._last_written_lsn == 100
    assert client._queue.empty()


async def test_last_written_lsn_never_regresses():
    client = _client()

    big_xlog = XLogData(
        wal_start=500,
        wal_end=500,
        send_time=0,
        payload=_begin_payload(),
    )
    await client._handle_xlog_data(big_xlog)
    assert client._last_written_lsn == 500

    small_keepalive = PrimaryKeepalive(wal_end=100, send_time=0, reply_requested=False)
    await client._handle_keepalive(small_keepalive)
    assert client._last_written_lsn == 500

    big_keepalive = PrimaryKeepalive(wal_end=900, send_time=0, reply_requested=False)
    await client._handle_keepalive(big_keepalive)
    assert client._last_written_lsn == 900


async def test_handle_xlog_data_discards_type_message_without_enqueuing():
    client = _client()
    xlog = XLogData(wal_start=100, wal_end=999, send_time=0, payload=_type_payload())

    await client._handle_xlog_data(xlog)

    assert client._last_written_lsn == 100
    assert client._queue.empty()


async def test_handle_xlog_data_discards_origin_message_without_enqueuing():
    client = _client()
    xlog = XLogData(wal_start=100, wal_end=999, send_time=0, payload=_origin_payload())

    await client._handle_xlog_data(xlog)

    assert client._last_written_lsn == 100
    assert client._queue.empty()


def _keepalive_payload(
    wal_end: int = 777,
    send_time: int = 0,
    reply_requested: bool = False,
) -> bytes:
    return (
        b"k"
        + wal_end.to_bytes(8, "big")
        + send_time.to_bytes(8, "big")
        + bytes([1 if reply_requested else 0])
    )


async def test_handle_frame_dispatches_a_keepalive_message():
    client = _client()
    client._transport = AsyncMock()

    await client._handle_frame(_keepalive_payload(wal_end=777))

    assert client._last_written_lsn == 777


async def test_receive_loop_exits_immediately_when_already_closing():
    client = _client()
    client._transport = AsyncMock()
    client._closing.set()

    await client._receive_loop()

    client._transport.read.assert_not_awaited()
