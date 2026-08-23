# RFC 06 — Backpressure

**Status:** Implemented
**Documented:** 2026-08-23

## Depends on

- ARCHITECTURE.md (the correctness invariant: never claim durable progress ahead of
  what the application's handler has actually completed).
- Client Runtime (RFC 05) — this feature restructures its single run-loop task into
  two, and reuses its status-update timing helper and checkpoint/feedback wiring
  unchanged.

Streamed transactions (Transaction Assembly, RFC 03) are the traffic pattern most
likely to actually exercise this feature's bound in practice — a single logical
transaction can now arrive as many chunks over a long wall-clock period — but neither
feature depends on the other's implementation.

## Summary / Context

**Problem.** PostgreSQL can produce logical-replication traffic far faster than an
application's handler can consume it — the handler might call a slow external
service, write to a loaded downstream database, or simply be doing more work per
transaction than decoding one costs. Without a bound, a single task that decodes,
assembles, *and* calls the handler in one straight line means a slow handler call
blocks everything behind it, including replying to PostgreSQL's keepalives — risking
the connection being killed out from under the client by `wal_sender_timeout` for a
reason that has nothing to do with the network. The opposite failure mode is just as
real: buffering unboundedly while waiting for a slow handler to catch up would let
process memory grow without limit under sustained backpressure.

**Business value.** This is what lets walbox stay alive and responsive to
PostgreSQL — never appearing to have silently died from the server's perspective —
regardless of how slow or bursty the application's own downstream work is, while
keeping the process's own memory footprint bounded and predictable rather than
proportional to how far behind the handler has fallen.

## Goals and Non-Goals

**Goals:**
- Decouple "receiving and assembling transactions" from "handing them to the
  application's handler and checkpointing them" into two independently-scheduled
  units of work, so one being slow never blocks the other.
- Bound the amount of decoded-but-unprocessed data in memory to a configured
  maximum (`options.max_pending_transactions`).
- Keep sending PostgreSQL keepalive replies and periodic status updates even while
  backpressured, so a slow handler alone never trips `wal_sender_timeout`.
- Provide the mechanism a clean shutdown (Client Runtime, RFC 05) needs: a way to
  instantly unblock whichever side of the split is currently stuck (a full queue on
  the producer side, an empty queue on the consumer side).

**Non-Goals:**
- Not the *complete* graceful shutdown sequence on its own — this feature proves the
  unblocking mechanism works, not that the run loop returns without raising
  afterward. See Client Runtime (RFC 05) for the full clean-return sequence built on
  top of this feature's mechanism.
- No change to *what* gets checkpointed or reported in feedback — the same
  handler-then-checkpoint sequence Client Runtime already established runs
  unchanged, just in a different task than the one reading the socket.
- No accounting of streamed-transaction buffer memory (Transaction Assembly, RFC 03)
  against this feature's own queue bound — a large or numerous streamed transaction
  can still grow memory independently of `max_pending_transactions`. A known,
  stated v0.1 limitation (see README), not solved here.
- No multiple concurrent consumers. Exactly one consumer task keeps checkpointing
  (and therefore durable progress) trivially monotonic in commit order, with no
  reordering buffer needed — a deliberate simplicity/throughput trade-off, not an
  oversight.
- No graceful, application-visible drain of everything still queued at shutdown
  time — see Pros/Cons for why dropping unstarted queued work is the correct choice
  here, not an unfinished one.

## Proposed Design

**Two long-lived tasks under one task group**, replacing the single straight-line
loop: a receiver (decode → assemble → enqueue) and a consumer (dequeue → handler →
checkpoint), sharing one bounded queue.

```python
async def _receive_loop(self) -> None:
    while not self._closing.is_set():
        payload = await self._await_with_status_updates(self._transport.read())
        await self._handle_frame(payload)   # ... -> self._enqueue(transaction)

async def _consume_loop(self, handler: Handler) -> None:
    while True:
        try:
            transaction = await self._queue.get()
        except asyncio.QueueShutDown:
            return
        await self._process(transaction, handler)   # handler(...) then checkpoint.save(...)
```

The receiver's entire responsibility shrinks to "turn bytes into assembled
transactions and get them onto the queue without ever going silent for too long" —
calling the handler and attaching a checkpoint move entirely to the consumer.

**The backpressure-vs-feedback race.** When the queue is full, the receiver doesn't
just block on enqueueing — it keeps sending status updates on the existing schedule
for as long as it takes the consumer to make room, using the same
wait-with-periodic-status-updates helper Client Runtime (RFC 05) already built for
waiting on the next byte to read. It can't service a *newly arriving* keepalive while
blocked this way (it isn't back at reading the socket yet), but it doesn't need to —
an unsolicited periodic status update resets PostgreSQL's idea of "this client is
alive" exactly as well as a requested reply would. This only holds if the configured
status interval is comfortably shorter than PostgreSQL's own `wal_sender_timeout` —
an operational requirement documented for the user, not re-enforced in code.

**`close()`'s instant unblock.** A synchronous `close()` (callable directly from a
signal handler) needs to unblock a task that might be stuck in a blocking enqueue or
a blocking dequeue, without itself being able to await anything. Shutting the queue
down immediately makes both a pending enqueue and a pending dequeue raise right away
— the only way a synchronous method can instantly release a task blocked on either
operation.

## Pros / Cons

**Dropping queued-but-unstarted work immediately on shutdown, vs. draining it
first.** Draining would let already-decoded transactions finish being processed
before returning, but nothing in the queue at that point has been checkpointed yet
— it's safe to drop precisely because it will be redelivered after a reconnect or
restart anyway, under the same at-least-once guarantee the rest of the system
already relies on. Dropping it bounds shutdown latency to at most one in-flight
handler call, rather than however long a full queue would take to drain; draining
would trade a worse, unbounded shutdown latency for no correctness benefit, since
none of that dropped work was ever going to be lost permanently.

**Exactly one consumer task, vs. multiple concurrent consumers for higher
throughput.** Concurrent consumers would let independent transactions' handlers run
in parallel, but checkpointing (and therefore what "durable progress" means) would
need a reordering buffer to stay meaningful in commit order — a transaction
checkpointed out of order could make an earlier, still-in-flight transaction's work
look already covered by feedback that hasn't actually happened for it yet. Single
consumer keeps checkpoint order trivially equal to commit order, at the cost of
throughput being bounded by one handler invocation at a time — a deliberate v0.1
trade-off toward simplicity and correctness over raw throughput.

**A bounded queue with a hard cap, vs. an unbounded buffer that just lets memory
grow.** An unbounded buffer would never backpressure the receiver at all, but would
let a sufficiently slow handler consume unbounded memory — exactly the failure mode
this feature exists to prevent. The bound trades "the queue can, in principle, fill
up and slow ingestion" for "memory usage under a slow handler is predictable and
configurable," which is the right trade for a library whose worst case (an
unresponsive downstream sink) is expected to happen in production.

## Implementation

- `walbox/client.py` — `self._queue`, `self._closing`, `close()`, `_receive_loop`,
  `_consume_loop`, `_process`, `_await_with_status_updates`, `_enqueue`,
  `_handle_frame` (now handler-free — decode-and-enqueue only).

## Testing

- A transaction is enqueued without blocking when the queue has room; once the
  queue is full, enqueueing blocks until the consumer drains an item, and status
  updates keep going out on schedule the entire time the receiver is blocked this
  way — the direct proof that backpressure doesn't starve keepalive/feedback
  traffic.
- The shared wait-with-status-updates helper never duplicates or abandons the
  underlying operation it's waiting on across repeated timeout wakeups — verified
  by confirming the awaited operation only ever actually executes once, no matter
  how many status-update timeouts elapse first. This matters concretely for
  enqueueing: creating a *new* enqueue attempt on every timeout instead of
  re-awaiting the same one could eventually let two attempts both succeed once
  space appeared, silently double-enqueueing the same transaction.
- `close()` instantly unblocks both a receiver stuck enqueueing into a full queue
  and a consumer idle on an empty one, in each case within a short, bounded time
  rather than hanging.
- Against real PostgreSQL, with a small queue bound and a deliberately slow
  handler: the queue never exceeds its configured bound no matter how many
  transactions arrive; feedback sent while backpressured keeps reporting the
  pre-backpressure durable position, never any of the still-queued (and therefore
  not-yet-checkpointed) transactions' positions; and PostgreSQL's own view of
  replication lag for the slot grows during this period, proving the backpressure
  is real and externally visible, not just invisible to the local process.
