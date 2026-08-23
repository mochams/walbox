# RFC 05: Client Runtime (Connect, Feedback, Reconnect, Graceful Shutdown)

**Status:** Implemented
**Documented:** 2026-08-23

## Depends on

- ARCHITECTURE.md (error hierarchy: distinguishing `ReplicationConnectionError`,
  worth reconnecting over, from everything else, which isn't; the correctness
  invariant: never tell PostgreSQL a transaction is durably processed before the
  application's handler and its checkpoint are both durable).
- Replication Transport (RFC 04): `connect`/`create_slot_if_missing`/
  `start_replication`/`read`/`write`/`end_copy`/`close`.
- Wire Decoding (RFC 02): `decode_replication_message`, `encode_standby_status_update`.
- Checkpoint Store (RFC 01): `CheckpointStore.load()`/`CheckpointHandle`, the
  resume position and the durable-progress hook.
- Transaction Assembly (RFC 03): feeds decoded pgoutput messages in, receives
  assembled `Transaction`s out.

Backpressure (RFC 06) builds directly on top of this feature (the receiver/consumer
task split lives inside the same run loop this RFC establishes) rather than the
other way around.

## Summary / Context

**Problem.** Everything the other features provide (a transport, a decoder, an
assembler, a checkpoint store) is inert until something wires them into one running
process that: resumes from wherever it last durably left off; tells PostgreSQL what
it's received/durably processed so the server knows how much WAL it can safely
discard; recovers automatically from a dropped connection without skipping or
duplicating work beyond what at-least-once already allows; and stops cleanly on
request without losing or corrupting in-flight work. Each of these is a real, subtle
correctness concern on its own: resuming from the wrong position either replays too
much or (far worse) skips transactions; reporting flushed progress ahead of what's
actually durable violates walbox's core guarantee outright; a shutdown that just
kills the connection mid-handler risks losing track of exactly what did or didn't
complete.

**Business value.** This is the feature that makes walbox an unattended,
production-operable service rather than a demo script: an operator can restart the
process, lose the network connection, or send `SIGTERM` at any moment, and the
system's correctness guarantees (at-least-once delivery, no silent loss, no
premature acknowledgment) hold regardless of exactly when any of that happens.

## Goals and Non-Goals

**Goals:**
- On startup, resume `START_REPLICATION` from one past the last durable checkpoint
  (or the beginning, if there is none yet), never from wherever a prior connection
  happened to disconnect.
- Reply to PostgreSQL's keepalives promptly enough to never trip
  `wal_sender_timeout`, and additionally send proactive, periodic status updates so
  silence never exceeds a configured interval even between keepalives.
- Report flushed/applied progress that only ever reflects what's *actually*
  durably checkpointed, via both the client's own auto-checkpoint path and an
  application calling `tx.checkpoint.save(...)` directly.
- On a lost connection, reconnect with exponential backoff, always re-reading the
  current durable checkpoint on every attempt (which may have advanced since the
  last attempt).
- On `close()` (safe to call from a signal handler), stop accepting new work,
  finish any in-flight handler call, checkpoint it, send one final status update
  reflecting that, close the replication stream cleanly, and return from `run()`
  with no exception raised.

**Non-Goals:**
- No decoding of pgoutput messages, or assembly of raw messages into transactions.
  This feature just calls into Wire Decoding (RFC 02) and Transaction Assembly
  (RFC 03).
- No bounded queue or receiver/consumer task split of its own. See Backpressure
  (RFC 06), which is layered directly on top of this feature's run loop.
- No `Update`/`Delete`/streaming-specific handling of any kind. This feature is
  agnostic to which pgoutput message kinds exist; that's Wire Decoding's and
  Transaction Assembly's concern entirely.
- No jitter on the reconnect backoff delay. A fixed exponential sequence (1s,
  doubling, capped at 60s) is simple and sufficient for v0.1; if multiple walbox
  consumers reconnecting in lockstep against the same PostgreSQL instance ever
  becomes a real operational problem, jitter is a small, separate addition to make
  later.
- Only `ReplicationConnectionError` triggers a reconnect attempt. A `ProtocolError`,
  `DecodeError`, `CheckpointError`, or an exception raised by the application's own
  handler all propagate out of `run()` immediately and end it. None of these
  indicate a transient network condition a retry would fix, and retrying them
  anyway would risk quietly masking a real bug.
- No graceful, application-level drain of everything still queued at shutdown time.
  See Backpressure (RFC 06) for what happens to queued-but-not-yet-started work
  when `close()` is called.
- Does not make shutdown of a *completely idle* connection instantaneous. An idle
  receiver is only guaranteed to notice `close()` within one status-update
  interval, not immediately. A connection with any ongoing traffic, or one
  currently backpressured, exits promptly; it's only true idleness that has this
  bound, judged an acceptable trade-off against the complexity of a second,
  always-alive wakeup mechanism purely to shave that bound down further.

## Proposed Design

### Startup and resume position

```python
checkpoint_lsn = await self.options.checkpoint_store.load()
if checkpoint_lsn is None:
    start_lsn = 0          # a freshly created (or already-caught-up) slot
else:
    start_lsn = checkpoint_lsn + 1   # resume from *after* the last durably processed byte
```

The checkpoint value is stored as a raw, un-adjusted "last byte actually processed"
position (Transaction Assembly's convention, RFC 03); `START_REPLICATION`'s LSN
parameter wants the opposite convention: "resume from here, meaning everything
before this is done," matching PostgreSQL's own `confirmed_flush_lsn` semantics.
Requesting the checkpoint value itself, unadjusted, would make PostgreSQL redeliver
the walbox's own last-processed transaction on every single reconnect. This is
re-read from scratch (not cached from a prior connection attempt) every single
time a connection is (re-)established, which matters directly for reconnect: it's
what makes every retry resume from whatever is *currently* durable, not from
whatever was durable when the process first started.

### Decoding and dispatch

```python
async def _handle_xlog_data(self, xlog: XLogData) -> None:
    self._last_written_lsn = max(self._last_written_lsn, xlog.wal_start)
    pgoutput_message = self._decoder.decode(xlog.payload)
    if isinstance(pgoutput_message, Type | Origin):
        return  # decoded fully, not actionable -- logged and dropped here, never reaching assembly
    transaction = self._assembler.feed(pgoutput_message)
    if transaction is not None:
        await self._enqueue(transaction)  # or `await handler(transaction)` pre-Backpressure
```

`_last_written_lsn` tracks `xlog.wal_start` (the position of data actually received),
never `xlog.wal_end` (the server's own overall WAL position, which can legitimately
be ahead of what any given message contains). This is confirmed directly against
PostgreSQL's own `pg_recvlogical` client source, which advances its own tracked
position from the same field and explicitly treats `wal_end` as informational only. A
keepalive's own `wal_end` field *does* advance the same tracked value, since a
keepalive has no "start of this data" field of its own and the server's overall
position is exactly what's needed to keep that value from going stale during
otherwise-idle periods.

### Feedback: reporting only what's actually durable

```python
async def _send_status_update(self, *, reply_requested: bool) -> None:
    update = StandbyStatusUpdate(
        written_lsn=self._last_written_lsn,
        flushed_lsn=self._durable_lsn,
        applied_lsn=self._durable_lsn,
        client_time=pg_now_micros(),
        reply_requested=reply_requested,
    )
    await self._transport.write(encode_standby_status_update(update))
```

`self._durable_lsn` only ever advances through one path: a small callback attached
to every `CheckpointHandle` (Checkpoint Store, RFC 01), invoked *after* the
underlying `store.save(...)` call has already completed. This is the load-bearing
correctness property of the whole feature: there is no code path that lets a status
update report a position PostgreSQL wasn't already durably told about locally,
first. Because the callback fires from `CheckpointHandle.save` itself rather than
from client code specific to one mode, both `manage_checkpoint=True`'s automatic
save and an application's own `manage_checkpoint=False` manual save advance the same
tracked value identically, with no special-casing per mode.

Status updates are sent both reactively (a keepalive with `reply_requested=True`)
and proactively, on a timer, so PostgreSQL never sees silence longer than
`options.status_interval` even during a period with no keepalive at all. Both waits
(for the next byte to read, and, once Backpressure exists, for queue space) share
one helper that races the underlying wait against the status-update deadline,
sending an unsolicited status update and resetting the deadline on every timeout
without abandoning or duplicating the underlying wait.

### Reconnect: an outer retry loop around one connection's lifetime

```python
async def run(self, handler: Handler) -> None:
    self._next_backoff = _INITIAL_BACKOFF
    while not self._closing.is_set():
        try:
            await self._run_once(handler)
        except ReplicationConnectionError as exc:
            if self._closing.is_set():
                return
            await self._reconnect_delay(exc)
```

`_run_once` owns one connection's entire lifetime: re-reading the checkpoint,
connecting, replicating, and (once it returns normally) the shutdown sequence below.
Backoff starts at 1 second, doubles, caps at 60 seconds, and resets to the initial
value the moment a connection gets far enough to be considered healthy, so a single
transient blip doesn't permanently slow down recovery from a later, unrelated one.
`run()` checks `self._closing` both in its loop condition and right after catching a
`ReplicationConnectionError`, so a disconnection that happens to coincide with
`close()` being called doesn't sleep through a pointless backoff cycle before
noticing the shutdown was already requested.

### Graceful shutdown: turning "both tasks stopped" into a clean return

`close()` (safe to call directly from a signal handler, since it does no I/O, only
flips state) sets a `closing` flag and shuts down the delivery queue immediately
(Backpressure, RFC 06, owns the queue itself; this feature's receive loop and the
consumer loop it drives both react to the same shutdown signal). The receive loop
checks `self._closing` between every message and at every status-update wakeup, so it
notices and returns promptly whenever there's any traffic to process; a completely
idle connection notices within one status-update interval, since that's the loop's
only other wakeup source. Once both the receiving and processing sides have
genuinely finished (meaning any transaction that was already being handled when
`close()` was called has completed and been checkpointed), the run loop, on this
clean path only, sends one final status update (now correctly reflecting whatever
was just checkpointed), ends the COPY BOTH stream in an orderly way, and returns from
`run()` with no exception raised.

## Pros / Cons

**Fixed exponential backoff, vs. jittered backoff.** Jitter would reduce the risk of
many walbox consumers reconnecting to the same PostgreSQL instance in lockstep after
a shared network blip, but adds a small amount of complexity for a problem not yet
observed to matter at v0.1's expected scale. Deferred rather than solved
speculatively.

**Reporting the honest floor during periods with nothing yet checkpointed, vs.
optimistically reporting the last *received* position.** Reporting `received` as if
it were `flushed` would let PostgreSQL discard WAL slightly sooner, but would violate
the correctness invariant outright the moment a crash happened between "received" and
"actually processed and checkpointed": the entire reason this invariant exists is to
prevent exactly that class of silent, unrecoverable gap. Never on the table as a real
option, but worth stating explicitly as a rejected shortcut.

**A shutdown sequence that only runs on the exception-free path, vs. attempting a
best-effort final status update even on error.** Only sending final feedback when the
run loop's own tasks returned cleanly (rather than trying to also send *something* on
an error path) keeps the shutdown sequence simple and its correctness easy to reason
about: it only ever runs when both tasks are known to have finished with nothing
in-flight and nothing corrupted. A crash-path attempt at final feedback would add
complexity for a case (a real error, not a requested shutdown) where sending
feedback isn't actually expected or required. The client just reconnects instead.

## Implementation

- `walbox/client.py`: `ReplicationClient.run`, `_run_once`, `_handle_frame`,
  `_handle_xlog_data`, `_handle_keepalive`, `_send_status_update`,
  `_record_durable_progress`, `_reconnect_delay`, `_next_backoff_value`, `close`,
  `_new_transport`.

## Testing

- Startup with no prior checkpoint starts replication from position zero and claims
  zero durable progress; an existing checkpoint starts replication one position past
  it, with the checkpoint value itself as the durable floor: the load-bearing
  regression case for the resume-position arithmetic, since starting from the
  checkpoint value itself (rather than one past it) would cause PostgreSQL to
  redeliver the last-processed transaction on every reconnect.
- `_last_written_lsn` tracks the position of data actually received (`wal_start`),
  never the server's own overall WAL position (`wal_end`) from a data message, and
  never regresses even if fed values out of order; a keepalive's `wal_end` still
  advances it regardless of whether that keepalive requested a reply.
- A keepalive that doesn't request a reply is not replied to, but still updates
  tracked position; one that does request a reply produces exactly one status
  update, reporting the durable floor for flushed/applied and the actually-received
  position for written.
- Durable progress only ever advances (never regresses, even if an application bug
  calls `save()` with a smaller value than already recorded) and only advances
  *after* the underlying checkpoint save has genuinely completed, proven by
  checking call order, not just the end state.
- Both `manage_checkpoint=True`'s automatic save and an application's own manual
  `manage_checkpoint=False` save advance reported feedback identically; a
  `manage_checkpoint=False` application that never calls `save()` at all leaves
  feedback pinned at the startup floor indefinitely, proving feedback genuinely
  tracks checkpoint state rather than transaction-processing activity.
- A dropped connection retries with doubling backoff and eventually succeeds,
  resuming correctly; a non-connection error (a decode failure, a handler
  exception) is never retried and propagates immediately, with no reconnect attempt
  made at all; backoff resets to its initial value after a connection is
  successfully re-established, rather than continuing to grow across unrelated
  failures.
- Against real PostgreSQL: terminating the backend mid-stream and resuming
  afterward keeps processing correctly; a transaction whose handler ran but whose
  checkpoint was deliberately withheld before a simulated disconnect is redelivered
  identically after reconnect, and, once the handler is allowed to checkpoint it,
  is not redelivered a third time on a further disconnect, proving redelivery
  happens exactly when the checkpoint is genuinely missing, never unconditionally;
  a transaction fully checkpointed immediately before a disconnect is not
  redelivered, and a subsequent transaction is still delivered correctly afterward.
- `close()` called while idle returns from `run()` within a short, bounded time
  with no exception; called while a handler is actively processing a transaction,
  `run()` doesn't return until that handler has completed and its checkpoint has
  been saved; called during an in-progress reconnection attempt, no further backoff
  sleep or reconnect attempt is made. The final status update is sent, and the
  replication stream is ended in an orderly way, strictly after both the
  receiving and processing sides have genuinely stopped, never before.
- Against real PostgreSQL, `SIGTERM`-equivalent shutdown behaves correctly in all
  four states an operator might trigger it in: idle, actively receiving a steady
  trickle of transactions, mid-handler on one specific transaction, and
  backpressured (verified jointly with Backpressure, RFC 06).
