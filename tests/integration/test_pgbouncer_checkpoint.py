"""`PostgresCheckpointStore` behavior when only reachable through pgbouncer.

Only `session` and `transaction` pool_mode are exercised here: `statement`
pool_mode can't run any checkpoint store query at all (see
docs/production/setup.md#pgbouncer for why), so there's nothing left to
test once that's established.

Ordinary SQL (unlike the replication protocol in test_pgbouncer_replication.py)
is exactly the kind of traffic pgbouncer is built to pool, but psycopg3's
autoprepare (`prepare_threshold`, default 5) is pool_mode-sensitive in a way
that only surfaces against a real pgbouncer, not by reading the code: it
promotes a repeated query to a named server-side prepared statement after
enough executions on the same client connection object. `from_pool()`
reuses a long-lived connection across many `save()`/`load()` calls, exactly
the pattern that triggers it. Prepared-statement names are assigned
sequentially per connection object (`_pg3_0`, `_pg3_1`, ...), not derived
from the query text. Under pgbouncer's transaction pool_mode a shared
backend can be handed to a *different* client connection between
transactions, so two clients' independently-named autoprepared statements
can collide on the same backend. Modern pgbouncer (>= 1.21.0) has a direct
fix for this: `max_prepared_statements`, which makes pgbouncer itself track
and re-prepare a client's named statements on whatever backend it's routed
to. `tests/integration/conftest.py`'s main pgbouncer image defaults it to
200, so plain `pgbouncer_dsn` never hits the collision below. The dedicated
`pgbouncer_no_prepared_statements_dsn` fixture turns that off, to prove the
collision is real for anyone who explicitly disables it (or runs an older
pgbouncer that never had it) and that the psycopg-side fix still works
there.
"""

import asyncio
import uuid
from collections.abc import Awaitable
from collections.abc import Callable

import psycopg
import pytest
from psycopg_pool import AsyncConnectionPool

from walbox.checkpoint import PostgresCheckpointStore

pytestmark = pytest.mark.pgbouncer

# Past psycopg3's default `prepare_threshold` (5), so a connection reused
# across iterations would autoprepare if nothing disabled it.
_AUTOPREPARE_ITERATIONS = 15
_CONCURRENT_WORKERS = 3


def _unique_table() -> str:
    return f"walbox_checkpoint_{uuid.uuid4().hex}"


def _unique_consumer() -> str:
    return f"consumer_{uuid.uuid4().hex}"


async def _disable_autoprepare(conn: psycopg.AsyncConnection) -> None:
    conn.prepare_threshold = None


async def _create_checkpoint_table(dsn: str, table: str) -> None:
    """Pre-create the checkpoint table outside the concurrent workers below.

    `CREATE TABLE IF NOT EXISTS` isn't safe to run concurrently from
    multiple *uncommitted* sessions racing to create the same not-yet-
    existing table (a plain PostgreSQL catalog race, unrelated to
    pgbouncer). The table is created once, up front, so each concurrent
    worker's own `_ensure_schema` call is just a safe no-op against an
    already-existing table.
    """
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
        await conn.execute(
            f"CREATE TABLE IF NOT EXISTS {table} ("
            "consumer_name TEXT PRIMARY KEY, "
            "lsn BIGINT NOT NULL, "
            "updated_at TIMESTAMPTZ NOT NULL DEFAULT now())",
        )


@pytest.mark.timeout(30)
async def test_adhoc_roundtrip(pgbouncer_dsn: str, pool_mode: str) -> None:
    """Ad hoc `save()`/`load()`: each call opens and closes its own connection.

    That never accumulates enough repeats on one connection to autoprepare,
    regardless of pool_mode.
    """
    store = PostgresCheckpointStore(
        pgbouncer_dsn,
        consumer_name=_unique_consumer(),
        table=_unique_table(),
    )

    await store.save(42)
    assert await store.load() == 42


@pytest.mark.timeout(30)
async def test_caller_owned_transaction(pgbouncer_dsn: str, pool_mode: str) -> None:
    """`save(lsn, connection=...)` joining a caller's own open transaction.

    Designed so a caller can commit its own write and the checkpoint update
    atomically, which requires an open transaction block by definition.
    """
    async with await psycopg.AsyncConnection.connect(pgbouncer_dsn) as conn:
        store = PostgresCheckpointStore(
            pgbouncer_dsn,
            consumer_name=_unique_consumer(),
            table=_unique_table(),
        )

        await conn.execute("CREATE TABLE IF NOT EXISTS scratch (id INT)")
        await conn.execute("INSERT INTO scratch VALUES (1)")
        await store.save(7, connection=conn)
        await conn.commit()
        assert await store.load() == 7


async def _run_autoprepare_workers(
    dsn: str,
    *,
    configure: Callable[[psycopg.AsyncConnection], Awaitable[None]] | None = None,
) -> None:
    table = _unique_table()
    await _create_checkpoint_table(dsn, table)

    async def worker(
        pool: AsyncConnectionPool[psycopg.AsyncConnection],
        consumer_name: str,
    ) -> None:
        store = PostgresCheckpointStore.from_pool(
            pool,
            consumer_name=consumer_name,
            table=table,
        )
        for i in range(_AUTOPREPARE_ITERATIONS):
            await store.save(i)
            assert await store.load() == i
            await asyncio.sleep(0.01)

    async with AsyncConnectionPool(
        dsn,
        min_size=_CONCURRENT_WORKERS,
        max_size=_CONCURRENT_WORKERS,
        open=False,
        configure=configure,
    ) as pool:
        await pool.open(wait=True)
        await asyncio.gather(
            *(worker(pool, _unique_consumer()) for _ in range(_CONCURRENT_WORKERS)),
        )


@pytest.mark.timeout(60)
async def test_pooled_autoprepare_survives_contention(
    pgbouncer_dsn: str,
    pool_mode: str,
) -> None:
    """Concurrent `from_pool()` clients sharing a pgbouncer backend pool.

    session pool_mode ties one backend to one client connection for its
    whole lifetime, so autoprepare is invisible to pgbouncer entirely.
    transaction pool_mode can hand different backends to each transaction,
    which is where two clients' independently-named autoprepared
    statements could collide on a shared backend, except that the pgbouncer
    image this suite uses defaults `max_prepared_statements` to 200, which
    is exactly pgbouncer's own fix for that collision. No application-side
    workaround needed.
    """
    await _run_autoprepare_workers(pgbouncer_dsn)


@pytest.mark.timeout(60)
async def test_pooled_autoprepare_collides_without_max_prepared_statements(
    pgbouncer_no_prepared_statements_dsn: str,
) -> None:
    """The collision is real once `max_prepared_statements` is off.

    Same concurrent `from_pool()` contention as the test above, but against
    a transaction-mode pgbouncer with `max_prepared_statements = 0` (the
    old default, and still what you get from a pgbouncer older than
    1.21.0). This is what a caller who explicitly disables the setting, or
    hasn't upgraded pgbouncer, actually hits.
    """
    with pytest.raises(psycopg.ProgrammingError):
        await _run_autoprepare_workers(pgbouncer_no_prepared_statements_dsn)


@pytest.mark.timeout(60)
async def test_pooled_autoprepare_fixed_by_disabling_prepare_threshold(
    pgbouncer_no_prepared_statements_dsn: str,
) -> None:
    """The application-side fallback: disable autoprepare on your own pool.

    For a pgbouncer where `max_prepared_statements` isn't available or
    isn't set, `PostgresCheckpointStore.from_pool()` never touches the
    caller-owned pool's connection settings itself (the pool may be shared
    with the app's own unrelated queries), so callers must disable
    autoprepare themselves via `configure=`.
    """
    await _run_autoprepare_workers(
        pgbouncer_no_prepared_statements_dsn,
        configure=_disable_autoprepare,
    )
