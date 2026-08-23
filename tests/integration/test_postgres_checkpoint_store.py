"""Integration tests for `PostgresCheckpointStore`, using a real Postgres.

This store's entire reason to exist is the same-transaction pattern
(`save(lsn, connection=conn)` joining the caller's own open transaction), so
it cannot be meaningfully unit-tested -- every test here needs a real
Postgres connection.
"""

import uuid

import pytest
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from walbox.checkpoint import PostgresCheckpointStore

pytestmark = pytest.mark.postgres


def _unique_consumer_name() -> str:
    return f"consumer_{uuid.uuid4().hex}"


def _unique_table_name() -> str:
    return f"walbox_checkpoint_{uuid.uuid4().hex}"


@pytest.fixture
async def sink_table(postgres_dsn: str, table: str):
    async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
        await conn.execute(f"CREATE TABLE {table}_sink (value TEXT)")
        yield
        await conn.execute(f"DROP TABLE IF EXISTS {table}_sink")


@pytest.fixture
def table() -> str:
    return _unique_table_name()


async def test_load_returns_none_before_first_save(
    postgres_dsn: str, table: str
) -> None:
    store = PostgresCheckpointStore(
        postgres_dsn,
        consumer_name=_unique_consumer_name(),
        table=table,
    )

    assert await store.load() is None


async def test_save_without_connection_is_durable_and_loadable(
    postgres_dsn: str,
    table: str,
) -> None:
    consumer_name = _unique_consumer_name()
    store = PostgresCheckpointStore(
        postgres_dsn, consumer_name=consumer_name, table=table
    )

    await store.save(100)

    fresh_store = PostgresCheckpointStore(
        postgres_dsn, consumer_name=consumer_name, table=table
    )
    assert await fresh_store.load() == 100


async def test_save_with_connection_commits_atomically_with_caller_transaction(
    postgres_dsn: str,
    table: str,
    sink_table: None,
) -> None:
    consumer_name = _unique_consumer_name()
    store = PostgresCheckpointStore(
        postgres_dsn, consumer_name=consumer_name, table=table
    )

    async with await AsyncConnection.connect(postgres_dsn) as conn:
        await conn.execute(f"INSERT INTO {table}_sink (value) VALUES ('sink-row')")
        await store.save(100, connection=conn)
        await conn.commit()

    async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
        cursor = await conn.execute(f"SELECT count(*) FROM {table}_sink")
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == 1
    assert await store.load() == 100


async def test_save_with_connection_creates_the_table_on_first_use(
    postgres_dsn: str,
    table: str,
) -> None:
    """Regression test: `save(connection=...)` used to skip schema creation
    entirely, so calling it as a store's very first operation (no prior
    `load()` or connection-less `save()`) raised because the backing table
    never existed. The `CREATE TABLE IF NOT EXISTS` it now runs is left
    uncommitted, same as the upsert, so it only becomes durable once the
    caller commits -- proven here by committing and then loading it back.
    """
    consumer_name = _unique_consumer_name()
    store = PostgresCheckpointStore(
        postgres_dsn, consumer_name=consumer_name, table=table
    )

    async with await AsyncConnection.connect(postgres_dsn) as conn:
        await store.save(100, connection=conn)
        await conn.commit()

    assert await store.load() == 100


async def test_save_with_connection_rolls_back_with_caller_transaction(
    postgres_dsn: str,
    table: str,
    sink_table: None,
) -> None:
    consumer_name = _unique_consumer_name()
    store = PostgresCheckpointStore(
        postgres_dsn, consumer_name=consumer_name, table=table
    )
    await store.load()

    async with await AsyncConnection.connect(postgres_dsn) as conn:
        await conn.execute(f"INSERT INTO {table}_sink (value) VALUES ('sink-row')")
        await store.save(100, connection=conn)
        await conn.rollback()

    async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
        cursor = await conn.execute(f"SELECT count(*) FROM {table}_sink")
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == 0
    assert await store.load() is None


async def test_save_with_connection_does_not_commit_itself(
    postgres_dsn: str,
    table: str,
) -> None:
    consumer_name = _unique_consumer_name()
    store = PostgresCheckpointStore(
        postgres_dsn, consumer_name=consumer_name, table=table
    )
    await store.load()

    commit_calls = 0

    async with await AsyncConnection.connect(postgres_dsn) as conn:
        real_commit = conn.commit

        async def spy_commit() -> None:
            nonlocal commit_calls
            commit_calls += 1
            await real_commit()

        conn.commit = spy_commit  # type: ignore[method-assign]

        await store.save(100, connection=conn)
        assert commit_calls == 0

        await conn.commit()
        assert commit_calls == 1


async def test_distinct_consumer_names_do_not_clobber_each_other(
    postgres_dsn: str,
    table: str,
) -> None:
    store_a = PostgresCheckpointStore(
        postgres_dsn, consumer_name="consumer-a", table=table
    )
    store_b = PostgresCheckpointStore(
        postgres_dsn, consumer_name="consumer-b", table=table
    )

    await store_a.save(100)
    await store_b.save(200)

    assert await store_a.load() == 100
    assert await store_b.load() == 200


async def test_repeated_save_upserts_same_row(postgres_dsn: str, table: str) -> None:
    consumer_name = _unique_consumer_name()
    store = PostgresCheckpointStore(
        postgres_dsn, consumer_name=consumer_name, table=table
    )

    await store.save(100)
    await store.save(200)

    assert await store.load() == 200
    async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
        cursor = await conn.execute(
            f"SELECT count(*) FROM {table} WHERE consumer_name = %s",
            (consumer_name,),
        )
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] == 1


async def test_schema_created_lazily_on_first_load(
    postgres_dsn: str, table: str
) -> None:
    async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
        cursor = await conn.execute(
            "SELECT to_regclass(%s)",
            (table,),
        )
        row = await cursor.fetchone()
        assert row is not None
        assert row[0] is None

    store = PostgresCheckpointStore(
        postgres_dsn,
        consumer_name=_unique_consumer_name(),
        table=table,
    )

    assert await store.load() is None


async def test_from_pool_load_and_save_round_trip_via_a_real_pool(
    postgres_dsn: str, table: str
) -> None:
    consumer_name = _unique_consumer_name()
    async with AsyncConnectionPool(
        postgres_dsn, min_size=1, max_size=2, open=False
    ) as pool:
        store = PostgresCheckpointStore.from_pool(
            pool, consumer_name=consumer_name, table=table
        )

        assert await store.load() is None

        await store.save(100)

        assert await store.load() == 100


async def test_from_pool_save_with_connection_is_unaffected_by_the_pool(
    postgres_dsn: str,
    table: str,
    sink_table: None,
) -> None:
    """The pool only backs ad hoc `load()`/connection-less `save()` calls --
    the same-transaction pattern still needs the caller's own connection,
    pool or not, exactly as it does for a plain `dsn`-constructed store.
    """
    consumer_name = _unique_consumer_name()
    async with AsyncConnectionPool(
        postgres_dsn, min_size=1, max_size=2, open=False
    ) as pool:
        store = PostgresCheckpointStore.from_pool(
            pool, consumer_name=consumer_name, table=table
        )

        async with await AsyncConnection.connect(postgres_dsn) as conn:
            await conn.execute(f"INSERT INTO {table}_sink (value) VALUES ('sink-row')")
            await store.save(100, connection=conn)
            await conn.commit()

        assert await store.load() == 100
