"""The end-to-end walbox replication client.

Wires `transport.py`, `protocol.py`, `pgoutput.py`, and `transaction.py`
together into two long-lived tasks under one `asyncio.TaskGroup`: a receiver
(socket, protocol, pgoutput, transaction assembly, enqueue onto a bounded
queue) and a consumer (dequeue, handler, checkpoint). Splitting the two
means a slow `handler` can't block PostgreSQL keepalive and feedback
traffic; the receiver keeps servicing the socket and sending status updates
even while backpressured on a full queue.
"""

import asyncio
import contextlib
import logging
import time
from collections.abc import Awaitable
from collections.abc import Callable
from typing import TypeVar

from walbox.abc import CheckpointHandle
from walbox.abc import CheckpointStore
from walbox.abc import Metrics
from walbox.abc import Transaction
from walbox.abc import WalboxOptions
from walbox.errors import ReplicationConnectionError
from walbox.pgoutput import Decoder
from walbox.pgoutput import Message
from walbox.pgoutput import Origin
from walbox.pgoutput import Type
from walbox.protocol import PrimaryKeepalive
from walbox.protocol import StandbyStatusUpdate
from walbox.protocol import XLogData
from walbox.protocol import decode_replication_message
from walbox.protocol import encode_standby_status_update
from walbox.protocol import pg_now_micros
from walbox.transaction import TransactionAssembler
from walbox.transport import ReplicationTransport

Handler = Callable[[Transaction, CheckpointHandle], Awaitable[None]]

logger = logging.getLogger("walbox.client")

_INITIAL_BACKOFF = 1.0
_MAX_BACKOFF = 60.0

_T = TypeVar("_T")


def _next_backoff_value(current: float) -> float:
    """Double `current`, capped at `_MAX_BACKOFF`.

    Returns:
        The next backoff delay, in seconds.
    """
    return min(current * 2, _MAX_BACKOFF)


class WalboxClient:
    """Consumes a PostgreSQL logical replication stream and dispatches transactions."""

    def __init__(
        self,
        options: WalboxOptions,
        checkpoint_store: CheckpointStore,
    ) -> None:
        """Initialize with the client's replication options and checkpoint store."""
        self.options = options
        self.checkpoint_store = checkpoint_store
        self._transport: ReplicationTransport | None = None
        self._decoder = Decoder()
        self._assembler = TransactionAssembler()
        self._last_written_lsn = 0
        self._durable_lsn = 0
        self._next_backoff = _INITIAL_BACKOFF
        self._queue: asyncio.Queue[Transaction] = asyncio.Queue(
            options.max_pending_transactions,
        )
        self._closing = asyncio.Event()
        self._next_status_at = 0.0  # set for real at the top of each _run_once
        # Metrics-only counters/gauges, kept separate from `_last_written_lsn`
        # above, which also absorbs each keepalive's `wal_end` for feedback
        # purposes and so can't double as "how much WAL has actually been
        # received", the quantity `replication_lag_bytes` needs to be
        # meaningful.
        self._receive_lsn = 0
        self._transactions_processed = 0
        self._changes_processed = 0
        self._reconnect_count = 0
        self._last_handler_latency = 0.0
        self._last_keepalive_wal_end = 0
        self._last_keepalive_at = 0.0
        self._last_checkpoint_latency = 0.0
        self._transactions_since_checkpoint = 0

    def _new_transport(self) -> ReplicationTransport:
        return ReplicationTransport(
            self.options.dsn,
            self.options.slot_name,
            self.options.publication_name,
        )

    def close(self) -> None:
        """Signal the running client to stop.

        Safe to call directly from `loop.add_signal_handler`, since it's a
        plain callback rather than a coroutine. Does no I/O: it just sets
        `_closing` and shuts down `self._queue`, which unblocks a receiver
        stuck on a full-queue `put()` or a consumer idle on an empty
        `get()`. Turning that into a clean, non-raising exit is `run()`'s
        job elsewhere in this class.
        """
        logger.info("shutdown requested", extra={"slot": self.options.slot_name})
        self._closing.set()
        self._queue.shutdown(immediate=True)

    async def run(self, handler: Handler) -> None:
        """Connect, start replication, and dispatch decoded transactions forever.

        `handler` is called once per assembled `Transaction`, in commit
        order, with a `CheckpointHandle` bound to this run's checkpoint
        store. The handler is responsible for calling `checkpoint.save(...)`
        itself; walbox never checkpoints automatically.

        On a `ReplicationConnectionError`, waits out an exponentially
        growing backoff and retries, always resuming from the durable
        checkpoint rather than wherever the dropped connection left off.
        Any other exception propagates uncaught, ending `run`.
        """
        self._next_backoff = _INITIAL_BACKOFF
        while not self._closing.is_set():
            try:
                await self._run_once(handler)
            except ReplicationConnectionError as exc:
                if self._closing.is_set():
                    return
                await self._reconnect_delay(exc)

    async def _run_once(self, handler: Handler) -> None:
        checkpoint_lsn = await self.checkpoint_store.load()
        if checkpoint_lsn is None:
            start_lsn = 0
            self._durable_lsn = 0
        else:
            start_lsn = checkpoint_lsn + 1
            self._durable_lsn = checkpoint_lsn
        self._transport = self._new_transport()
        logger.info(
            "connecting",
            extra={
                "slot": self.options.slot_name,
                "publication": self.options.publication_name,
            },
        )
        await self._transport.connect()
        await self._transport.create_slot_if_missing()
        await self._transport.start_replication(start_lsn)
        logger.info(
            "subscribed",
            extra={
                "slot": self.options.slot_name,
                "publication": self.options.publication_name,
                "lsn": start_lsn,
            },
        )
        self._next_backoff = _INITIAL_BACKOFF
        self._next_status_at = time.monotonic() + self.options.status_interval
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._receive_loop(), name="walbox-receiver")
                tg.create_task(self._consume_loop(handler), name="walbox-consumer")
        except* ReplicationConnectionError as excgroup:
            # TaskGroup always raises an (Base)ExceptionGroup, even for a
            # single failing child task. Unwrap so `run`'s plain `except
            # ReplicationConnectionError` keeps seeing exactly the exception
            # type it always has. Only the receiver's transport calls ever
            # raise this, so exactly one is ever in the group.
            raise excgroup.exceptions[0] from None
        # Only reached once both tasks have returned normally, i.e. only on a
        # clean, close()-triggered shutdown.
        logger.info(
            "shutdown: sending final status update",
            extra={"slot": self.options.slot_name},
        )
        # Final feedback, now reflecting anything the consumer just checkpointed.
        await self._send_status_update(reply_requested=False)
        logger.info(
            "shutdown: ending replication stream",
            extra={"slot": self.options.slot_name},
        )
        await self._transport.end_copy()
        self._transport.close()
        logger.info("shutdown complete", extra={"slot": self.options.slot_name})

    async def _reconnect_delay(self, exc: ReplicationConnectionError) -> None:
        self._reconnect_count += 1
        logger.warning(
            "replication connection lost, reconnecting in %.1fs: %s",
            self._next_backoff,
            exc,
        )
        await asyncio.sleep(self._next_backoff)
        self._next_backoff = _next_backoff_value(self._next_backoff)

    async def _receive_loop(self) -> None:
        assert self._transport is not None
        try:
            while not self._closing.is_set():
                payload = await self._await_with_status_updates(self._transport.read())
                await self._handle_frame(payload)
        except asyncio.QueueShutDown:
            # close() was called while a put() was pending: expected, not an error.
            return

    async def _handle_frame(self, payload: bytes) -> None:
        message = decode_replication_message(payload)
        match message:
            case XLogData():
                await self._handle_xlog_data(message)
            case PrimaryKeepalive():
                await self._handle_keepalive(message)
            case _:  # pragma: no cover, exhaustive over ReplicationMessage
                unreachable = f"unhandled replication message type {message!r}"
                raise AssertionError(unreachable)

    async def _handle_xlog_data(self, xlog: XLogData) -> None:
        self._last_written_lsn = max(self._last_written_lsn, xlog.wal_start)
        self._receive_lsn = max(self._receive_lsn, xlog.wal_start)
        pgoutput_message = self._decoder.decode(xlog.payload)
        if isinstance(pgoutput_message, Type | Origin | Message):
            # Decoded fully (so the byte stream never desyncs) but not
            # actionable for the outbox pattern: logged and dropped here
            # rather than forwarded to the assembler.
            logger.debug(
                "discarding unactionable pgoutput message: %r",
                pgoutput_message,
            )
            return
        transaction = self._assembler.feed(pgoutput_message)
        if transaction is not None:
            await self._enqueue(transaction)

    async def _await_with_status_updates(self, awaitable: Awaitable[_T]) -> _T:
        """Await `awaitable`, sending status updates while it waits.

        Sends a non-advancing status update every `options.status_interval`
        seconds so PostgreSQL never sees silence for longer than that, no
        matter how long `awaitable` takes. Also checks `self._closing` on
        each wake-up, which is what bounds an idle receiver's shutdown
        latency to one `status_interval` (a pending `transport.read()` on an
        otherwise idle connection has no other way to notice `close()` was
        called).

        Returns:
            Whatever `awaitable` resolves to.

        Raises:
            asyncio.QueueShutDown: If `self._closing` is set while still
                waiting.
        """
        task = asyncio.ensure_future(awaitable)
        try:
            while True:
                timeout = max(self._next_status_at - time.monotonic(), 0)
                done, _pending = await asyncio.wait({task}, timeout=timeout)
                if task in done:
                    return task.result()
                if self._closing.is_set():
                    raise asyncio.QueueShutDown
                await self._send_status_update(reply_requested=False)
                await self._maybe_report_metrics()
                self._next_status_at = time.monotonic() + self.options.status_interval
        finally:
            # If we're leaving for any reason other than `task` itself
            # completing (QueueShutDown above, or this coroutine being
            # cancelled directly, e.g. a caller cancelling `run()`'s task
            # instead of calling `close()`), `task` is still pending and
            # must be cancelled and awaited here, not left to leak.
            # Otherwise its `_wait_readable`/`_wait_writable` never reaches
            # its own `finally` block, leaving a stale
            # `loop.add_reader`/`add_writer` registration on a file
            # descriptor that can be silently reused by a later connection
            # once this one closes. Reproducible in a real crash, but only
            # on epoll (Linux): closing a socket implicitly drops it from
            # epoll at the kernel level while the selector's own bookkeeping
            # doesn't know that happened, so the next connection to reuse
            # that fd number collides with a stale entry
            # (`FileNotFoundError` from `EpollSelector.modify`). kqueue
            # (macOS) tolerates this, which is why it's invisible in local
            # runs and only surfaces in Linux CI.
            if not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task

    async def _enqueue(self, transaction: Transaction) -> None:
        try:
            self._queue.put_nowait(transaction)
        except asyncio.QueueFull:
            logger.info("backpressure engaged", extra={"lsn": transaction.commit_lsn})
            await self._await_with_status_updates(self._queue.put(transaction))
            logger.info("backpressure relieved", extra={"lsn": transaction.commit_lsn})

    async def _consume_loop(self, handler: Handler) -> None:
        while True:
            try:
                transaction = await self._queue.get()
            except asyncio.QueueShutDown:
                return
            await self._process(transaction, handler)

    async def _process(self, transaction: Transaction, handler: Handler) -> None:
        checkpoint = CheckpointHandle(
            self.checkpoint_store,
            self._record_durable_progress,
            transaction.commit_lsn,
        )
        self._transactions_processed += 1
        self._transactions_since_checkpoint += 1
        self._changes_processed += len(transaction.changes)
        handler_started_at = time.monotonic()
        await handler(transaction, checkpoint)
        self._last_handler_latency = time.monotonic() - handler_started_at
        logger.debug(
            "%d transaction(s) processed since last checkpoint save",
            self._transactions_since_checkpoint,
            extra={
                "transactions_since_checkpoint": self._transactions_since_checkpoint,
            },
        )

    def _record_durable_progress(self, lsn: int, latency: float) -> None:
        self._transactions_since_checkpoint = 0
        self._durable_lsn = max(self._durable_lsn, lsn)
        self._last_checkpoint_latency = latency

    async def _maybe_report_metrics(self) -> None:
        """Invoke `options.on_metrics`, if set, with a fresh `Metrics` snapshot.

        `on_metrics` must not raise; if it does, the exception is caught and
        logged here rather than taking down the replication loop.
        """
        if self.options.on_metrics is None:
            return
        try:
            self.options.on_metrics(self._current_metrics())
        except Exception:
            logger.exception("metrics callback raised an exception")

    def _current_metrics(self) -> Metrics:
        return Metrics(
            consumer_name=self.options.consumer_name,
            receive_lsn=self._receive_lsn,
            checkpoint_lsn=self._durable_lsn,
            replication_lag_bytes=self._last_keepalive_wal_end - self._receive_lsn,
            transactions_processed=self._transactions_processed,
            changes_processed=self._changes_processed,
            reconnect_count=self._reconnect_count,
            last_handler_latency_seconds=self._last_handler_latency,
            queue_depth=self._queue.qsize(),
            last_keepalive_at=self._last_keepalive_at,
            last_checkpoint_latency_seconds=self._last_checkpoint_latency,
            transactions_since_checkpoint=self._transactions_since_checkpoint,
        )

    async def _send_status_update(self, *, reply_requested: bool) -> None:
        update = StandbyStatusUpdate(
            written_lsn=self._last_written_lsn,
            flushed_lsn=self._durable_lsn,
            applied_lsn=self._durable_lsn,
            client_time=pg_now_micros(),
            reply_requested=reply_requested,
        )
        assert self._transport is not None
        await self._transport.write(encode_standby_status_update(update))

    async def _handle_keepalive(self, keepalive: PrimaryKeepalive) -> None:
        self._last_written_lsn = max(self._last_written_lsn, keepalive.wal_end)
        self._last_keepalive_wal_end = keepalive.wal_end
        self._last_keepalive_at = time.monotonic()
        logger.debug("keepalive received", extra={"lsn": keepalive.wal_end})
        if keepalive.reply_requested:
            await self._send_status_update(reply_requested=False)
            logger.debug("keepalive reply sent", extra={"lsn": keepalive.wal_end})
