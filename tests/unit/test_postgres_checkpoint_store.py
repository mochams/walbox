"""Unit tests for `PostgresCheckpointStore`, mocking the Postgres connection.

`tests/integration/test_postgres_checkpoint_store.py` proves the real
same-transaction behavior against a real Postgres. These tests instead mock
`psycopg.AsyncConnection.connect` so every branch -- own-connection vs
caller-supplied `connection=`, schema-creation-once, row-found vs
row-missing -- is exercised without Docker.
"""

from unittest.mock import AsyncMock

import psycopg

from walbox.checkpoint import PostgresCheckpointStore


class _FakeCursor:
    def __init__(self, row: tuple[int] | None) -> None:
        self._row = row

    async def fetchone(self) -> tuple[int] | None:
        return self._row


class _FakeConnection:
    def __init__(self, row: tuple[int] | None = None) -> None:
        self.executed: list[str] = []
        self.committed = 0
        self._row = row

    async def execute(self, query: object, params: object = ()) -> _FakeCursor:
        self.executed.append(str(query))
        return _FakeCursor(self._row)

    async def commit(self) -> None:
        self.committed += 1

    async def __aenter__(self) -> "_FakeConnection":
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


def _create_table_calls(conn: _FakeConnection) -> list[str]:
    return [sql for sql in conn.executed if "CREATE TABLE" in sql]


async def test_load_returns_none_when_no_row_exists(monkeypatch):
    fake_conn = _FakeConnection(row=None)
    monkeypatch.setattr(
        psycopg.AsyncConnection,
        "connect",
        AsyncMock(return_value=fake_conn),
    )
    store = PostgresCheckpointStore("dsn", consumer_name="consumer")

    assert await store.load() is None
    assert len(_create_table_calls(fake_conn)) == 1


async def test_load_returns_the_saved_lsn_when_a_row_exists(monkeypatch):
    fake_conn = _FakeConnection(row=(100,))
    monkeypatch.setattr(
        psycopg.AsyncConnection,
        "connect",
        AsyncMock(return_value=fake_conn),
    )
    store = PostgresCheckpointStore("dsn", consumer_name="consumer")

    assert await store.load() == 100


async def test_save_without_connection_opens_its_own_connection_and_commits(
    monkeypatch,
):
    fake_conn = _FakeConnection()
    monkeypatch.setattr(
        psycopg.AsyncConnection,
        "connect",
        AsyncMock(return_value=fake_conn),
    )
    store = PostgresCheckpointStore("dsn", consumer_name="consumer")

    await store.save(100)

    # One commit from `_ensure_schema` creating the table, one from `save` itself.
    assert fake_conn.committed == 2
    assert len(_create_table_calls(fake_conn)) == 1
    assert len(fake_conn.executed) == 2  # CREATE TABLE + upsert


async def test_save_with_connection_upserts_without_its_own_connection_or_commit(
    monkeypatch,
):
    connect_mock = AsyncMock()
    monkeypatch.setattr(psycopg.AsyncConnection, "connect", connect_mock)
    store = PostgresCheckpointStore("dsn", consumer_name="consumer")
    conn = _FakeConnection()

    await store.save(100, connection=conn)

    connect_mock.assert_not_called()
    assert conn.committed == 0
    assert _create_table_calls(conn) == []
    assert len(conn.executed) == 1


async def test_ensure_schema_only_creates_the_table_once_per_store(monkeypatch):
    fake_conn = _FakeConnection()
    monkeypatch.setattr(
        psycopg.AsyncConnection,
        "connect",
        AsyncMock(return_value=fake_conn),
    )
    store = PostgresCheckpointStore("dsn", consumer_name="consumer")

    await store.save(100)
    await store.save(200)

    assert len(_create_table_calls(fake_conn)) == 1
