# RFC 07: Observability

**Status:** Implemented
**Documented:** 2026-08-23

## Depends on

- ARCHITECTURE.md (error hierarchy: log records reuse `WalboxError`'s own context
  field names, so raised errors and log lines are trivially correlatable).
- Every other feature (RFC 01–06): this feature instruments the modules and
  control-flow paths they already established, rather than introducing new ones.

## Summary / Context

**Problem.** A replication client running unattended in production is, by default,
a black box: whether it's keeping up, how far behind it is, whether it's actively
reconnecting, how slow the application's own handler is; none of that is visible
from outside the process unless it's deliberately surfaced. Without it, diagnosing
"is this consumer healthy?" requires reading application logs the library itself
never wrote, or worse, inferring health from PostgreSQL's own side (replication slot
lag, WAL retention growing) after a problem has already been running for a while.

**Business value.** Structured logs and a small set of counters/gauges are what let
an operator answer "is this thing keeping up, and if not, where is it stuck?"
without instrumenting the library themselves, while imposing zero mandatory
dependencies and zero behavioral change on applications that don't care to look.

## Goals and Non-Goals

**Goals:**
- Structured logging across the whole pipeline (transport, protocol/pgoutput,
  transaction assembly, client, checkpoint), using field names consistent with the
  error hierarchy's own context fields.
- An optional metrics callback exposing point-in-time counters/gauges covering
  receive position, checkpoint position, replication lag, throughput
  (transactions/changes processed), reconnect count, handler latency, queue depth,
  last keepalive time, and checkpoint latency.
- Zero impact on correctness or behavior: purely additive instrumentation.

**Non-Goals:**
- No bundled metrics exporter (no Prometheus/StatsD/OpenTelemetry integration).
  Just a plain synchronous callback; wiring it to any specific system is the
  application's job.
- No new dependency for structured logging (no `structlog`, no `loguru`): standard
  library `logging` only, using its `extra=` mechanism, so it keeps working with
  whatever log aggregator/formatter the application already has configured.
- No new timer or task purely for metrics: the callback piggybacks on the
  existing periodic status-update mechanism (Client Runtime, RFC 05), rather than
  spinning up a second competing schedule.
- No historical aggregation (rolling windows, percentiles, rate calculations). The
  callback receives point-in-time values; any windowing is the application's job if
  it wants one.
- No redesign of the error hierarchy's context fields (ARCHITECTURE.md): this
  feature reuses those field names for log records, it doesn't invent new ones.

## Proposed Design

### `Metrics` and the callback

```python
@dataclass(frozen=True)
class Metrics:
    receive_lsn: int
    checkpoint_lsn: int
    replication_lag_bytes: int
    transactions_processed: int
    changes_processed: int
    reconnect_count: int
    last_handler_latency_seconds: float
    queue_depth: int
    last_keepalive_at: float
    last_checkpoint_latency_seconds: float

MetricsCallback = Callable[[Metrics], None]
```

Every counter is a plain instance attribute on the client, incremented at the exact
point in the existing run loop where that event already happens, with no restructuring
of the loop itself, only additions immediately before/after existing lines. The
callback is deliberately synchronous, not a coroutine: it's meant for a cheap side
effect (increment a counter object, push a gauge), not for the application to do
`await`-based work in response. An application that needs async work in reaction
should schedule that itself rather than have walbox await an arbitrary hook in the
middle of its own run loop. A callback that raises is caught and logged at the one
call site that invokes it: the single deliberate, narrow exception to the project's
general "don't bury error handling in generic except blocks" rule, justified because
this is third-party application code with a documented "must not raise" contract,
and its failure must not be allowed to affect correctness-critical control flow.

### Getting replication lag right

The obvious approach, reusing the client's existing "last written LSN" feedback field
as the "received" side of the lag calculation, turns out to be wrong. That field is
deliberately advanced by *both* incoming data and incoming keepalives (that's correct
for what it's for: telling PostgreSQL what's been received). But a keepalive's own
position gets folded into that same field the instant that keepalive is processed, so
subtracting it from itself would resolve to zero immediately after every keepalive
and go *negative* as more data arrives before the next one, the opposite of a
meaningful "how far behind is the client" signal. The fix is a second, metrics-only
tracked position, advanced only by incoming data messages and never touched by
keepalives, so the lag calculation reflects genuine received-vs-server-advertised
distance rather than an artifact of when the last keepalive happened to arrive.

### Logging conventions

- One logger per module, named after it.
- Structured context passed via `extra={...}`, using the same field names as the
  error hierarchy's own context object, so a log line and a raised error about the
  same event are trivially correlatable by anyone grepping or aggregating both.
- Level discipline: debug for per-message/per-keepalive volume; info for lifecycle
  transitions (connect, reconnect, shutdown steps, backpressure engaged/relieved);
  warning for documented-but-abnormal situations the client recovers from on its
  own (a lost connection about to be retried); error/exception only at a boundary
  that's about to re-raise or has just caught something genuinely unexpected.

## Pros / Cons

**Plain stdlib `logging`, vs. adopting `structlog`/`loguru`.** A dedicated
structured-logging library would make structured fields more ergonomic to emit and
query, but adds a new mandatory dependency to a library whose entire dependency
footprint is otherwise just `psycopg`. Standard library `logging`'s `extra=`
mechanism achieves the same structured-field outcome and composes with whatever
logging setup an application already has, at the cost of slightly more verbose call
sites.

**A synchronous metrics callback, vs. an async hook awaited from the run loop.** An
async hook would let an application do real I/O (e.g. push to a metrics backend)
directly inside the callback, but that means the replication run loop would be
blocked on arbitrary application-controlled I/O every time metrics are reported:
precisely the kind of coupling Backpressure (RFC 06) exists to avoid elsewhere in
the same loop. A synchronous callback is intentionally restrictive: cheap side
effects only, with the application responsible for scheduling anything heavier
itself.

**No bundled metrics exporter.** Bundling one (even an optional extra) would give
users a working Prometheus/StatsD integration out of the box, but commits the
library to maintaining and versioning against a specific ecosystem's client library.
Left entirely to the application for v0.1, matching the broader "very small public
API" principle the whole project follows.

## Implementation

- `walbox/abc.py`: `Metrics`, `MetricsCallback`, `ReplicationOptions.on_metrics`.
- `walbox/client.py`: per-event counters, `_maybe_report_metrics`,
  `_current_metrics`, the second `_receive_lsn` tracked position, lifecycle/
  reconnect/shutdown logging.
- `walbox/transport.py`: connect/slot-creation/`START_REPLICATION` logging.
- `walbox/protocol.py`: malformed-frame error logging before re-raising.
- `walbox/pgoutput.py`: relation-cache insert/overwrite logging, Type/Origin
  consumed-and-dropped logging.
- `walbox/transaction.py`: transaction open/commit/abort logging, including
  subxid-scoped `StreamAbort` discard details.
- `walbox/checkpoint.py`: `save()`/`load()` outcome and latency logging.

## Testing

- The metrics callback, when configured, is invoked with counters that reflect
  what actually happened: driving a couple of transactions through produces the
  expected transactions/changes-processed progression.
- A callback that raises is caught and logged, and does not stop replication from
  continuing; this is verified by confirming the run loop is still alive and processing
  afterward.
- Leaving the callback unconfigured (the default) costs nothing observable beyond
  the existing periodic timer still firing on schedule.
- Replication lag is computed from the corrected, keepalive-independent received
  position, not the feedback-facing "last written" field; verified with a
  synthetic keepalive and a known received position producing the expected lag
  value, distinct from what the (wrong) naive calculation would produce.
- Queue depth reported in metrics matches the bounded queue's actual current size
  at the moment of reporting.
- Against real PostgreSQL: running one transaction through with a log-capturing
  handler attached produces at least one log record carrying that transaction's
  xid and commit LSN in its structured context, proving the field-name
  correlation with the error hierarchy actually holds in practice, not just in
  isolated unit tests.
