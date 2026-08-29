# Contributing

For what walbox is and why its tooling is chosen the way it is, see
[`PROJECT.md`](PROJECT.md). For the system design, see
[`ARCHITECTURE.md`](ARCHITECTURE.md).

## Environment setup

walbox uses [`uv`](https://docs.astral.sh/uv/) for dependency management. Clone the
repo and run:

```sh
uv sync
```

This installs the dev dependency group (`psycopg[binary]`, `testcontainers`,
`cryptography`, `pytest` and its plugins, `ruff`, `pyrefly`, `prek`) alongside the
runtime dependency (`psycopg`).

Integration tests require Docker (via `testcontainers`) to start a real PostgreSQL
instance. Unit tests don't.

## Running checks

```sh
make test              # full suite: unit + integration (needs Docker)
make test-unit          # unit tests only, no Docker needed
make test-integration    # integration tests only (needs Docker)
make lint                # ruff check .
make format              # ruff format .
make typecheck           # pyrefly check
make run_precommit       # prek run --all-files
```

`make test` is the full gate and is what CI (and `run_precommit`) expects to pass.

### The coverage gate

The project enforces `--cov-fail-under=100` (branch coverage, not just line
coverage) on the full suite. This is a real gate, not aspirational: a change that
drops coverage below 100% fails `make test`. If a code path is genuinely
untestable (an abstract method body, a `TYPE_CHECKING` block, a `__main__` guard),
carve it out explicitly via `[tool.coverage.report] exclude_also` in
`pyproject.toml` rather than writing a test that doesn't actually exercise
anything meaningful, or leaving the gate lowered.

### Integration tests

walbox's own engineering principle is that replication correctness cannot be
verified against mocked messages alone. Any change to `transport.py`,
`protocol.py`, `pgoutput.py`, or `transaction.py` that touches wire-level behavior
should have a real-PostgreSQL integration test under `tests/integration/`, not just
a unit test against hand-crafted bytes. Integration test files set
`pytestmark = pytest.mark.postgres` explicitly at module level.

## Code style

These are hard constraints, not suggestions, carried over from the project's
original design brief:

- **One responsibility per function.** If a function both parses protocol bytes and
  mutates replication state, split it.
- **Keep functions short**: ideally 5-20 lines. A longer function needs a good
  reason.
- **No clever abstractions prematurely.** Introduce a class/Protocol only when it
  represents a real boundary (e.g. `RelationCache`'s statefulness in an otherwise
  pure decoding module).
- **Separate protocol, state, and policy**: see `ARCHITECTURE.md`'s module table.
  `transport.py` never decodes pgoutput; `transaction.py` never does I/O;
  `client.py` is the only place policy decisions (when to reconnect, when to
  checkpoint) live.
- **Prefer immutable value objects** for changes, transactions, and every
  replication message (`frozen=True, slots=True` dataclasses throughout).
- **Keep the public API tiny.** `walbox.__all__` is a frozen, deliberately short
  list; see README.md's Status section before adding to it.
- **Comments explain why, not what.** No comment restating what the next line
  obviously does (e.g. `# save checkpoint` above `checkpoint.save(...)`). A comment
  earns its place by capturing a hidden constraint, a subtle invariant, or a
  correction found by testing against real PostgreSQL that isn't obvious from the
  code alone.
- **Type everything**, especially protocol boundaries.
- **Avoid giant "manager"/"handler"/"processor" classes.** `client.py`'s
  `Client` is the closest thing to one and is kept to delivery-lifecycle
  orchestration only: decoding, assembly, and checkpointing all live in their own
  modules.
- **Don't bury error handling in generic `except Exception` blocks.** The one
  deliberate exception is the metrics callback boundary: a misbehaving
  `on_metrics` callback must never take down the replication loop.
- **Make shutdown and cancellation explicit**, not an emergent side effect of
  cleanup code scattered across callbacks.

## Pull requests

- Keep changes scoped to one feature or fix: this project favors small, reviewable
  units of work over broad rewrites.
- Add tests before or alongside the implementation, not after.
- Run `make test lint typecheck` (or `make run_precommit`) before opening a PR.
- If the change affects a documented design decision, update the relevant docs in
  the same PR rather than letting them drift from what's actually implemented.
- User-facing changes (new behavior, a fix, a breaking change) get an entry under
  `## [Unreleased]` in [`CHANGELOG.md`](CHANGELOG.md), in the same PR.
