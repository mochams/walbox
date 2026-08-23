"""Replication transport: owns the libpq COPY BOTH connection end to end.

`ReplicationTransport` opens a libpq replication connection, idempotently
creates the replication slot, issues `START_REPLICATION`, and drives the
COPY BOTH stream -- for the entire connection lifetime -- through libpq's
own COPY functions, exposed via Psycopg 3's `connection.pgconn`. asyncio's
only role anywhere in this module is to wait for the connection's file
descriptor to become readable or writable; libpq remains the sole owner of
the socket, its buffering, and (if negotiated) TLS, for the whole
connection -- this is what keeps replication traffic working over a TLS
connection at all, since bypassing libpq's own I/O functions would mean
reading and writing raw ciphertext. No `socket.socket` is ever constructed
around `pgconn.socket`, and no `recv`/`send`/`sock_recv`/`sock_sendall` call
appears anywhere here -- `pgconn.socket` is used exclusively as a readiness
token for `loop.add_reader`/`add_writer`.
"""

import asyncio
import logging
from typing import Any

from psycopg import AsyncConnection
from psycopg import OperationalError
from psycopg.conninfo import make_conninfo
from psycopg.pq import DiagnosticField
from psycopg.pq import ExecStatus
from psycopg.pq import PGconn
from psycopg.pq import PGresult

from walbox.errors import ErrorContext
from walbox.errors import ReplicationConnectionError

logger = logging.getLogger("walbox.transport")

_DUPLICATE_OBJECT_SQLSTATE = "42710"


def _format_lsn(lsn: int) -> str:
    """Render an LSN as PostgreSQL's hex `XXXXXXXX/XXXXXXXX` text form.

    Args:
        lsn: The LSN to render.

    Returns:
        The hex `XXXXXXXX/XXXXXXXX` text form.
    """
    return f"{lsn >> 32:X}/{lsn & 0xFFFFFFFF:X}"


def _parse_lsn(text: str) -> int:
    """Parse PostgreSQL's hex `XXXXXXXX/XXXXXXXX` LSN text form to an int.

    Args:
        text: The hex `XXXXXXXX/XXXXXXXX` text form to parse.

    Returns:
        The parsed LSN.
    """
    high, low = text.split("/")
    return (int(high, 16) << 32) | int(low, 16)


class ReplicationTransport:
    """Drives one PostgreSQL logical replication connection end to end.

    Owns exactly one `AsyncConnection` for its entire lifetime -- connection
    setup, slot creation, `START_REPLICATION`, the COPY BOTH read/write
    loop, and teardown all go through the same `pgconn`, never a raw socket.
    """

    def __init__(self, dsn: str, slot_name: str, publication_name: str) -> None:
        """Initialize with connection parameters; does not connect yet."""
        self._dsn = dsn
        self._slot_name = slot_name
        self._publication_name = publication_name
        self._conn: AsyncConnection[Any] | None = None

    @property
    def _pgconn(self) -> PGconn:
        """The underlying `pgconn`, valid once `connect()` has succeeded."""
        assert self._conn is not None
        return self._conn.pgconn

    async def connect(self) -> None:
        """Open a libpq replication connection.

        Raises:
            ReplicationConnectionError: If the connection could not be
                established.
        """
        replication_dsn = make_conninfo(self._dsn, replication="database")
        logger.debug(
            "connecting replication socket",
            extra={"slot": self._slot_name, "publication": self._publication_name},
        )
        try:
            self._conn = await AsyncConnection.connect(
                replication_dsn,
                autocommit=True,
            )
        except OperationalError as exc:
            logger.warning(
                "failed to open replication connection: %s",
                exc,
                extra={"slot": self._slot_name, "publication": self._publication_name},
            )
            message = f"failed to open replication connection: {exc}"
            raise ReplicationConnectionError(
                message,
                context=ErrorContext(
                    slot=self._slot_name,
                    publication=self._publication_name,
                ),
            ) from exc
        logger.info(
            "replication socket connected",
            extra={"slot": self._slot_name, "publication": self._publication_name},
        )

    async def _wait_readable(self) -> None:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        fd = self._pgconn.socket
        loop.add_reader(fd, lambda: future.done() or future.set_result(None))
        try:
            await future
        finally:
            loop.remove_reader(fd)

    async def _wait_writable(self) -> None:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        fd = self._pgconn.socket
        loop.add_writer(fd, lambda: future.done() or future.set_result(None))
        try:
            await future
        finally:
            loop.remove_writer(fd)

    async def _flush(self) -> None:
        try:
            await self._flush_until_complete()
        except OperationalError as exc:
            message = f"failed to flush: {exc}"
            raise ReplicationConnectionError(
                message,
                context=ErrorContext(slot=self._slot_name),
            ) from exc

    async def _flush_until_complete(self) -> None:
        pgconn = self._pgconn
        while pgconn.flush() == 1:
            # PQflush's own documented contract: the server can be blocked
            # trying to send us data and won't read ours until we read its --
            # waiting on write-ready alone can deadlock, so race both.
            readable = asyncio.ensure_future(self._wait_readable())
            writable = asyncio.ensure_future(self._wait_writable())
            done, pending = await asyncio.wait(
                {readable, writable},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if readable in done:
                pgconn.consume_input()

    async def _drain_result(self) -> PGresult:
        pgconn = self._pgconn
        while pgconn.is_busy():
            await self._wait_readable()
            pgconn.consume_input()

        result = None
        while (next_result := pgconn.get_result()) is not None:
            result = next_result
            # Once a COPY_IN/COPY_OUT/COPY_BOTH result comes back, libpq's
            # asyncStatus stays in that COPY_* state rather than resetting --
            # a further get_result() call doesn't wait on real I/O, it
            # fabricates another copy-status PGresult out of thin air, every
            # time, forever (PQgetResult -> getCopyResult in libpq's
            # fe-exec.c). psycopg's own generators.py hits this same libpq
            # behavior and guards it identically. Must stop here and let the
            # caller switch to the COPY data API instead.
            if result.status in {
                ExecStatus.COPY_IN,
                ExecStatus.COPY_OUT,
                ExecStatus.COPY_BOTH,
            }:
                break

        assert result is not None  # a command was always sent before draining
        return result

    async def create_slot_if_missing(self) -> None:
        """Create the replication slot, tolerating it already existing.

        Raises:
            ReplicationConnectionError: If slot creation fails for any
                reason other than the slot already existing.
        """
        pgconn = self._pgconn
        logger.debug(
            "creating replication slot if missing",
            extra={"slot": self._slot_name},
        )
        pgconn.send_query(
            f"CREATE_REPLICATION_SLOT {self._slot_name} LOGICAL pgoutput".encode(),
        )
        await self._flush()
        result = await self._drain_result()
        if result.status == ExecStatus.FATAL_ERROR:
            if result.error_field(DiagnosticField.SQLSTATE) == (
                _DUPLICATE_OBJECT_SQLSTATE.encode()
            ):
                logger.debug(
                    "replication slot already exists",
                    extra={"slot": self._slot_name},
                )
                return
            detail = pgconn.error_message.decode(errors="replace")
            logger.warning(
                "failed to create replication slot: %s",
                detail,
                extra={"slot": self._slot_name},
            )
            message = f"failed to create replication slot: {detail}"
            raise ReplicationConnectionError(
                message,
                context=ErrorContext(slot=self._slot_name),
            )
        logger.info("replication slot created", extra={"slot": self._slot_name})

    async def start_replication(self, start_lsn: int) -> None:
        """Issue `START_REPLICATION` and enter COPY BOTH mode.

        Args:
            start_lsn: The LSN to start streaming from.

        Raises:
            ReplicationConnectionError: If the server does not enter
                COPY BOTH mode.
        """
        command = (
            f"START_REPLICATION SLOT {self._slot_name} LOGICAL "
            f"{_format_lsn(start_lsn)} "
            f"(proto_version '2', publication_names '{self._publication_name}', "
            f"streaming 'on')"
        )
        pgconn = self._pgconn
        logger.debug(
            "starting replication",
            extra={
                "slot": self._slot_name,
                "publication": self._publication_name,
                "lsn": start_lsn,
            },
        )
        pgconn.send_query(command.encode())
        await self._flush()
        result = await self._drain_result()
        if result.status != ExecStatus.COPY_BOTH:
            detail = pgconn.error_message.decode(errors="replace")
            logger.warning(
                "START_REPLICATION did not enter COPY BOTH mode: %s",
                detail,
                extra={
                    "slot": self._slot_name,
                    "publication": self._publication_name,
                    "lsn": start_lsn,
                },
            )
            message = f"START_REPLICATION did not enter COPY BOTH mode: {detail}"
            raise ReplicationConnectionError(
                message,
                context=ErrorContext(
                    slot=self._slot_name,
                    publication=self._publication_name,
                    lsn=start_lsn,
                ),
            )
        logger.info(
            "replication started",
            extra={
                "slot": self._slot_name,
                "publication": self._publication_name,
                "lsn": start_lsn,
            },
        )

    async def read(self) -> bytes:
        """Return one complete replication message payload.

        Returns:
            An `XLogData` ('w') or `PrimaryKeepaliveMessage` ('k') payload,
            with the outer CopyData envelope already stripped by libpq.
            Never partial; the caller needs no buffering.

        Raises:
            ReplicationConnectionError: If the stream ends or an error
                occurs while receiving.
        """
        try:
            return await self._read_until_complete()
        except OperationalError as exc:
            # A real dropped connection (e.g. pg_terminate_backend) is
            # detected as a clean -1 ("replication stream ended") by
            # _read_until_complete below, not as a get_copy_data() failure --
            # this branch is for the -2 case (a genuine read error, per
            # Psycopg 3's own wrapper).
            message = f"error receiving copy data: {exc}"
            raise ReplicationConnectionError(
                message,
                context=ErrorContext(slot=self._slot_name),
            ) from exc

    async def _read_until_complete(self) -> bytes:
        pgconn = self._pgconn
        while True:
            nbytes, data = pgconn.get_copy_data(1)
            if nbytes > 0:
                return bytes(data)
            if nbytes == -1:
                message = "replication stream ended"
                raise ReplicationConnectionError(
                    message,
                    context=ErrorContext(slot=self._slot_name),
                )
            # nbytes == 0: no complete row yet. PQgetCopyData's own documented
            # contract -- wait for read-ready, then consume_input, then retry.
            # (A negative-error result other than -1 surfaces as an
            # OperationalError from get_copy_data itself, not as a return
            # value here -- see Psycopg 3's own wrapper.)
            await self._wait_readable()
            pgconn.consume_input()

    async def write(self, payload: bytes) -> None:
        """Send one bare replication-protocol payload during COPY BOTH.

        Args:
            payload: The bare payload (e.g. an `encode_standby_status_update`
                result). Do not pre-wrap in a CopyData envelope --
                `put_copy_data` does that.

        Raises:
            ReplicationConnectionError: If sending fails.
        """
        pgconn = self._pgconn
        try:
            while pgconn.put_copy_data(payload) == 0:
                await self._wait_writable()
            await self._flush()
        except OperationalError as exc:
            message = f"failed to send data: {exc}"
            raise ReplicationConnectionError(
                message,
                context=ErrorContext(slot=self._slot_name),
            ) from exc

    async def end_copy(self) -> None:
        """Send CopyDone, ending the COPY BOTH stream in an orderly way.

        Raises:
            ReplicationConnectionError: If ending the copy fails.
        """
        pgconn = self._pgconn
        try:
            while pgconn.put_copy_end() == 0:
                await self._wait_writable()
            await self._flush()
        except OperationalError as exc:
            message = f"failed to end copy: {exc}"
            raise ReplicationConnectionError(
                message,
                context=ErrorContext(slot=self._slot_name),
            ) from exc

    def close(self) -> None:
        """Immediately and synchronously tear down the connection.

        Safe to call more than once. Uses `pgconn.finish()` rather than
        `await self._conn.close()`: it's synchronous (this method must be
        callable directly from `loop.add_signal_handler`, which invokes
        plain callbacks) and it doesn't attempt to cleanly wind down an
        in-progress command mid-COPY.
        """
        if self._conn is not None:
            self._conn.pgconn.finish()
            self._conn = None
