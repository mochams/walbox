# walbox RFCs

These documents record the design of walbox's features, end to end, one per feature.
Each is a historical, as-built record, not a forward-looking plan, and walbox's
actual source and tests are the authority if anything here ever disagrees with them.
For the system-level view (how these features fit together, the error hierarchy that
cuts across all of them), see [`ARCHITECTURE.md`](../ARCHITECTURE.md) at the repo
root.

| RFC | Title | Depends on |
|---|---|---|
| [01](rfc-01-checkpoint-store.md) | Checkpoint Store | ARCHITECTURE.md |
| [02](rfc-02-wire-decoding.md) | Wire Decoding: Protocol Framing and pgoutput | ARCHITECTURE.md, RFC 04 |
| [03](rfc-03-transaction-assembly.md) | Transaction Assembly (including Streaming) | ARCHITECTURE.md, RFC 02 |
| [04](rfc-04-replication-transport.md) | Replication Transport | ARCHITECTURE.md, [research](research/replication-transport-architecture.md) |
| [05](rfc-05-client-runtime.md) | Client Runtime: Connect, Feedback, Reconnect, Graceful Shutdown | ARCHITECTURE.md, RFC 01, 02, 03, 04 |
| [06](rfc-06-backpressure.md) | Backpressure | ARCHITECTURE.md, RFC 05 |
| [07](rfc-07-observability.md) | Observability | ARCHITECTURE.md, RFC 01–06 |

All seven are `Status: Implemented`.

## Supporting research

[`research/`](research/) holds primary-source investigation that informed a design
decision, kept for its evidence rather than as a design record itself:

- [Replication Transport: Architecture Research](research/replication-transport-architecture.md):
  the libpq/asyncio/TLS investigation behind RFC 04.

## Not RFCs

Project scaffolding and tooling setup, and compatibility/release information, don't
carry enough design weight to be RFCs; they're covered in
[`PROJECT.md`](../PROJECT.md) instead. Day-to-day development workflow (environment
setup, running tests, code style) is in [`CONTRIBUTING.md`](../CONTRIBUTING.md).
