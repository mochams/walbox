"""Unit tests for `Walbox`.

Pure, no Postgres: `build()` and `build_with_pool()` are both checked by
inspecting the wired `Client.options`. `build_with_pool()` is
given a fake `ConnectionPool`-shaped double, the same pattern
`test_postgres_checkpoint_store.py` uses for `from_pool`, since walbox no
longer constructs or imports any pool implementation itself.
"""

from walbox.abc import WalboxOptions
from walbox.builder import Walbox
from walbox.checkpoint import PostgresCheckpointStore
from walbox.client import Client


class _FakeConnection:
    async def __aenter__(self) -> "_FakeConnection":
        return self

    async def __aexit__(self, *exc_info: object) -> bool:
        return False


class _FakePool:
    """A minimal double standing in for an application-owned connection pool."""

    def __init__(self) -> None:
        self.checkout_count = 0

    def connection(self) -> _FakeConnection:
        self.checkout_count += 1
        return _FakeConnection()


def _options(**overrides: object) -> WalboxOptions:
    return WalboxOptions(**{
        "consumer_name": "app",
        "dsn": "postgresql://example",
        "slot_name": "slot",
        "publication_name": "walbox_pub",
        **overrides,
    })


def test_build_returns_a_replication_client_wired_from_walbox_options():
    def on_metrics(metrics: object) -> None:
        return None

    options = _options(
        max_pending_transactions=50,
        status_interval=5,
        on_metrics=on_metrics,
    )

    client = Walbox.build(options)

    assert isinstance(client, Client)
    assert isinstance(client.checkpoint_store, PostgresCheckpointStore)
    assert client.options.consumer_name == "app"
    assert client.options.dsn == "postgresql://example"
    assert client.options.slot_name == "slot"
    assert client.options.publication_name == "walbox_pub"
    assert client.options.max_pending_transactions == 50
    assert client.options.status_interval == 5
    assert client.options.on_metrics is on_metrics


def test_build_with_pool_wires_a_checkpoint_store_backed_by_the_given_pool():
    pool = _FakePool()

    client = Walbox.build_with_pool(_options(), pool)

    assert isinstance(client, Client)
    assert isinstance(client.checkpoint_store, PostgresCheckpointStore)
    assert client.checkpoint_store._acquire.__self__ is pool
    assert client.options.consumer_name == "app"
