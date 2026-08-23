# Project

walbox is a small, production-oriented, asyncio-first Python runtime for consuming
PostgreSQL logical replication directly from WAL, built primarily for the
transactional outbox pattern: write an outbox row in the same PostgreSQL transaction
as your business data, then stream committed outbox inserts to an external system
without polling the table and without `LISTEN`/`NOTIFY`. For install instructions,
the quickstart, and the API itself, see [`README.md`](README.md); for the system
design, see [`ARCHITECTURE.md`](ARCHITECTURE.md); for how to work on the codebase,
see [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Status and scope

walbox is pre-1.0. Its scope is deliberately narrow: a correct, asyncio-native
logical-replication reader with durable checkpointing, backpressure, reconnect, and
graceful shutdown — not a general ORM, not a message broker, not a metrics
framework. README.md's Status section covers the exact stable public surface and
what's still provisional; this document doesn't repeat that.

The project was built incrementally, one feature at a time, each proven against a
real PostgreSQL instance (via `testcontainers`) before moving to the next — never
attempting the whole library in one pass. [`docs/`](docs/README.md) records the
design decisions behind each feature as it stands today.

## Why these tools

- **[`uv`](https://docs.astral.sh/uv/)** for dependency management and the build
  backend — fast, reproducible resolution via `uv.lock`, no separate virtualenv
  tooling to configure.
- **Plain `psycopg` (Psycopg 3), no extras, as the only runtime dependency.**
  walbox must not force a specific libpq binding on its users — a production
  deployment may prefer the pure-C `psycopg-c` build against a system libpq, or
  `psycopg[binary]`'s bundled wheel. Pinning an extra in the core dependency would
  make that choice for every consumer of the library. Psycopg 2 is never a
  dependency, even indirectly — a hard project constraint from day one.
- **`ruff`** for both linting (`select = ["ALL"]`, with a short, deliberate ignore
  list) and formatting — one tool instead of a linter-plus-formatter pair, with
  `docs/` excluded from formatting (its embedded code fences are illustrative
  prose, not source to reformat).
- **`pyrefly`** for type checking, at `min-severity = "warn"`, excluding `tests/`,
  `docs/`, and `examples/` — protocol boundaries and value objects are fully typed;
  test code and the example script prioritize readability over satisfying strict
  type-checking.
- **`pytest` with `pytest-asyncio`, `pytest-cov` at a `--cov-fail-under=100`
  branch-coverage gate, `pytest-timeout`, and `pytest-xdist`** (parallelism
  disabled by default via `-n 0`, since most integration tests share one
  `testcontainers` Postgres instance). 100% coverage is enforced, not aspirational
  — the small number of genuinely untestable patterns (`if TYPE_CHECKING:`,
  `@abstractmethod` bodies, `__main__` guards) are carved out explicitly via
  `[tool.coverage.report] exclude_also` rather than by lowering the floor.
- **`testcontainers[postgres]`**, because this project's own engineering principle
  is that replication correctness cannot be verified against mocked messages alone
  — every feature that touches the wire protocol has a real-PostgreSQL integration
  test, not just a unit test against hand-crafted bytes.
- **`cryptography`** (dev-only), to generate a self-signed certificate for the
  TLS-specific integration test — the direct regression test for the reason
  Replication Transport ([RFC 04](docs/rfc-04-replication-transport.md)) is built
  the way it is.

## Development methodology

Each feature was implemented as its own small, focused, independently reviewable
unit of work, in dependency order, with tests added before or alongside the
implementation — never a broad, unreviewable rewrite. `docs/` records the result of
that process per feature, not the process itself.
