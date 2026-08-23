"""Unit tests for examples/outbox.py, mocking Postgres/broker/client entirely.

`tests/integration/test_outbox_example.py` proves the same `handle`/
`handle_with_atomic_checkpoint` functions against a real Postgres. These
tests exercise the same code without Docker, by mocking `publish_to_broker`,
`psycopg.AsyncConnection.connect`, and (for `main()`) the checkpoint store
and client classes themselves.
"""

from dataclasses import dataclass
from dataclasses import field
from typing import Any
from unittest.mock import AsyncMock

import psycopg

from examples import outbox
from walbox.abc import ChangeEvent
from walbox.abc import ChangeKind
from walbox.abc import CheckpointHandle
from walbox.abc import Transaction


@dataclass
class _FakeCheckpointStore:
    saved: list[tuple[int, object]] = field(default_factory=list)

    async def load(self) -> int | None:
        return None

    async def save(self, lsn: int, *, connection: object | None = None) -> None:
        self.saved.append((lsn, connection))


def _tx(changes: list[ChangeEvent], store: _FakeCheckpointStore) -> Transaction:
    return Transaction(
        xid=1,
        commit_lsn=999,
        commit_time=0,
        changes=changes,
        checkpoint=CheckpointHandle(store),
    )


async def test_publish_to_broker_returns_without_raising():
    await outbox.publish_to_broker({"a": 1})


async def test_handle_publishes_outbox_inserts_and_checkpoints(monkeypatch):
    published: list[dict[str, Any]] = []

    async def _fake_publish(payload: dict[str, Any]) -> None:
        published.append(payload)

    monkeypatch.setattr(outbox, "publish_to_broker", _fake_publish)

    store = _FakeCheckpointStore()
    change = ChangeEvent(kind=ChangeKind.INSERT, table="public.outbox", new={"id": 1})
    tx = _tx([change], store)

    await outbox.handle(tx)

    assert published == [{"id": 1}]
    assert store.saved == [(999, None)]


async def test_handle_skips_non_outbox_and_non_insert_changes(monkeypatch):
    published: list[dict[str, Any]] = []

    async def _fake_publish(payload: dict[str, Any]) -> None:
        published.append(payload)

    monkeypatch.setattr(outbox, "publish_to_broker", _fake_publish)

    other_table = ChangeEvent(
        kind=ChangeKind.INSERT, table="public.other", new={"id": 1}
    )
    non_insert = ChangeEvent(
        kind=ChangeKind.UPDATE, table="public.outbox", new={"id": 2}
    )
    store = _FakeCheckpointStore()
    tx = _tx([other_table, non_insert], store)

    await outbox.handle(tx)

    assert published == []
    assert store.saved == [(999, None)]


class _FakeAsyncConnection:
    def __init__(self) -> None:
        self.committed = 0

    async def commit(self) -> None:
        self.committed += 1

    async def __aenter__(self) -> "_FakeAsyncConnection":
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


async def test_handle_with_atomic_checkpoint_saves_and_commits_on_the_caller_connection(
    monkeypatch,
):
    fake_conn = _FakeAsyncConnection()
    connect_mock = AsyncMock(return_value=fake_conn)
    monkeypatch.setattr(psycopg.AsyncConnection, "connect", connect_mock)

    store = _FakeCheckpointStore()
    tx = _tx([], store)

    await outbox.handle_with_atomic_checkpoint(tx, "dsn")

    connect_mock.assert_awaited_once_with("dsn")
    assert store.saved == [(999, fake_conn)]
    assert fake_conn.committed == 1


async def test_main_wires_options_from_the_environment_and_runs_the_client(
    monkeypatch,
):
    monkeypatch.setenv("WALBOX_DSN", "postgresql://example")
    created_options = []
    ran_with = []

    class _FakeCheckpointStore2:
        def __init__(self, dsn: str, *, consumer_name: str) -> None:
            self.dsn = dsn
            self.consumer_name = consumer_name

    class _FakeClient:
        def __init__(self, options: object) -> None:
            created_options.append(options)

        def close(self) -> None:
            pass

        async def run(self, handler: object) -> None:
            ran_with.append(handler)

    monkeypatch.setattr(outbox, "PostgresCheckpointStore", _FakeCheckpointStore2)
    monkeypatch.setattr(outbox, "ReplicationClient", _FakeClient)

    await outbox.main()

    options = created_options[0]
    assert isinstance(options.checkpoint_store, _FakeCheckpointStore2)
    assert options.checkpoint_store.dsn == "postgresql://example"
    assert options.checkpoint_store.consumer_name == "outbox-consumer"
    assert options.dsn == "postgresql://example"
    assert options.slot_name == "outbox_slot"
    assert options.publication_name == "walbox_pub"
    assert options.manage_checkpoint is False
    assert ran_with == [outbox.handle]
