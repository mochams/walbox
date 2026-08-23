# Architecture

This is the current, as-built system design: the authoritative description of how
walbox's layers fit together. Where anything here disagrees with walbox's actual
source, the source wins.

## The pipeline

```
PostgreSQL
    │
    ▼
Replication Transport ── libpq/Psycopg 3, COPY BOTH, TLS-transparent
    │  bare, complete inner-message payloads (XLogData / PrimaryKeepaliveMessage)
    ▼
Wire Decoding ── protocol.py (outer messages) + pgoutput.py (inner pgoutput sub-protocol)
    │  Relation, Begin, Insert, Update, Delete, Truncate, Commit, Type, Origin,
    │  StreamStart, StreamStop, StreamCommit, StreamAbort
    ▼
Transaction Assembly ── buffers per-xid, emits only on commit, never on rollback
    │  Transaction(xid, commit_lsn, commit_time, changes)
    ▼
Bounded delivery queue (Backpressure)
    │
    ▼
Async application handler
    │
    ▼
Checkpoint Store ── durable local replay position
    │
    ▼
Replication feedback (Client Runtime) ── StandbyStatusUpdate, reflecting only
                                          what's actually durable
```

Each layer is a separate module with one responsibility: protocol, state, and
policy are kept apart rather than mixed into one place:

| Module | Owns |
|---|---|
| `walbox/transport.py` | libpq/socket mechanics: opening the connection, driving COPY BOTH |
| `walbox/protocol.py` | outer replication-message framing (XLogData/keepalive/status-update) |
| `walbox/pgoutput.py` | inner pgoutput sub-protocol decoding |
| `walbox/transaction.py` | transaction assembly, including streaming |
| `walbox/checkpoint.py` | durable local replay position |
| `walbox/client.py` | delivery lifecycle: connect, feedback, reconnect, backpressure, shutdown, metrics |
| `walbox/abc.py` | shared value objects and Protocols (`Transaction`, `ChangeEvent`, `CheckpointStore`, `Metrics`, `ReplicationOptions`) |
| `walbox/errors.py` | the error hierarchy (below) |

One deliberate deviation from the original brief: `protocol.py`'s scope shrank after
the [replication-transport research](docs/research/replication-transport-architecture.md)
established that libpq itself already owns CopyData/CopyDone envelope framing in
both directions. `protocol.py` never implements that framing; it only decodes/
encodes the messages *inside* an already-unwrapped payload. See
[RFC 04](docs/rfc-04-replication-transport.md) and
[RFC 02](docs/rfc-02-wire-decoding.md) for the full reasoning.

## The core correctness invariant

walbox's fundamental guarantee is **at-least-once transaction delivery with a
durable local replay position**, never end-to-end exactly-once. The one invariant
every layer above is built to uphold:

```
application effect completed successfully
        ↓
local checkpoint durably persisted
        ↓
replication flush feedback may advance
```

walbox must never tell PostgreSQL that a transaction has been durably processed
before the application has actually finished its handler *and* the corresponding
checkpoint has been made durable. A crash may cause replay; it must never cause
silent loss. Duplicates are acceptable and expected. Exactly-once *effects* (not
exactly-once *delivery*) are achieved by the application layering an idempotent or
deduplicating sink on top (see README.md's Exactly-once-effects section).

## Failure semantics

One row per crash point, each traceable to the RFC that guarantees it:

| Crash point | Outcome on restart |
|---|---|
| Before handler runs | Transaction fully redelivered once replication resumes from the durable checkpoint. |
| During handler execution | Redelivered in full; sink must tolerate a partially-applied-then-repeated attempt (dedupe on `outbox.id`). |
| After handler succeeds, before checkpoint saved | Redelivered: the canonical at-least-once duplicate. Durability is never sacrificed to avoid it. |
| During checkpoint save | The previous durable checkpoint remains valid (`FileCheckpointStore`'s atomic rename, or `PostgresCheckpointStore`'s transactional rollback leaving the prior committed row intact); transaction redelivered. |
| After checkpoint durable, before feedback sent | Feedback resumes from the checkpoint on reconnect; Postgres may redeliver already-checkpointed work if its own `confirmed_flush_lsn` lagged; this is tolerated, but the handler must stay idempotent regardless. |
| After feedback sent, before Postgres durably records it | Same as above: feedback is a hint for WAL retention/restart position, never the app's own source of truth for progress. |
| Mid-receive (partway through a WAL message) | The partial message is never assembled into a `Transaction`; full retransmission from the checkpoint on reconnect. |
| Mid-keepalive reply | No durable state was claimed; safe, since the connection simply times out and reconnects normally. |
| Mid-reconnect | Next attempt retries from the same durable checkpoint; a failed reconnect attempt claims no progress. |
| Mid-shutdown | Never worse than a plain crash at the same point in the sequence: whatever wasn't yet durable is redelivered, whatever was, isn't. |
| Mid-large/streamed transaction | The in-memory streamed-transaction buffer is lost; it was never durable by design. Postgres resends the entire transaction from scratch on reconnect; no partial/corrupt delivery. |

## Error hierarchy

One base exception, `WalboxError`, carries structured context (`slot`, `publication`,
`lsn`, `xid`, `relation`, `message_type`) that every raise site across every layer
above can attach to, plus four concrete subclasses every layer raises instead of ad
hoc `ValueError`/`RuntimeError`:

```python
class ProtocolError(
    WalboxError
): ...  # the byte stream or message sequence violated protocol expectations


class DecodeError(
    WalboxError
): ...  # a message's bytes couldn't be decoded into its expected shape


class ReplicationConnectionError(
    WalboxError
): ...  # the replication connection failed or was lost


class CheckpointError(WalboxError): ...  # a CheckpointStore failed to load or save
```

Context accumulates as an error crosses layers: a lower layer raises with whatever
it knows (Wire Decoding knows a malformed message's type, not which transaction it
belongs to); a higher layer calls `.enrich(xid=...)` in an `except` block before
re-raising, adding what it knows without needing to know about fields it doesn't
otherwise care about. This keeps each layer's raise sites honest about what they
actually know, rather than threading full context through every call site up front.

`ProtocolError` deliberately covers both byte-level framing violations (Wire
Decoding) and message-sequencing violations (Transaction Assembly); both are
fundamentally "the protocol's contract was violated," just detected in different
layers; splitting them wouldn't give callers a meaningfully different way to react.
`ReplicationConnectionError` is the one exception type Client Runtime's reconnect
loop treats as worth retrying; everything else indicates a genuine bug or a
corrupted stream, not a transient condition.

## Where each feature lives

| Layer | RFC |
|---|---|
| Checkpoint Store | [RFC 01](docs/rfc-01-checkpoint-store.md) |
| Wire Decoding (protocol framing + pgoutput) | [RFC 02](docs/rfc-02-wire-decoding.md) |
| Transaction Assembly (including streaming) | [RFC 03](docs/rfc-03-transaction-assembly.md) |
| Replication Transport | [RFC 04](docs/rfc-04-replication-transport.md) |
| Client Runtime (connect, feedback, reconnect, shutdown) | [RFC 05](docs/rfc-05-client-runtime.md) |
| Backpressure | [RFC 06](docs/rfc-06-backpressure.md) |
| Observability | [RFC 07](docs/rfc-07-observability.md) |

For "how do I use walbox," see [`README.md`](README.md). For "how do I work on
walbox," see [`CONTRIBUTING.md`](CONTRIBUTING.md). For project status and tooling
rationale, see [`PROJECT.md`](PROJECT.md).
