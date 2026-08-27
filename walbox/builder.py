"""Builds a fully wired `WalboxClient` from public `WalboxOptions`."""

from walbox.abc import WalboxOptions
from walbox.checkpoint import ConnectionPool
from walbox.checkpoint import PostgresCheckpointStore
from walbox.client import WalboxClient


class WalboxBuilder:
    """Constructs a `WalboxClient` without exposing checkpoint-store wiring."""

    @staticmethod
    def build(options: WalboxOptions) -> WalboxClient:
        """Build a client whose checkpoint store opens ad hoc connections.

        Opens and closes a new connection for every `checkpoint.save()`
        call. Prefer `build_with_pool` unless you have a reason not to add
        the `psycopg-pool` dependency; this is here for that case, and for
        checkpoint volume low enough that the per-call connection cost
        doesn't matter.

        Returns:
            A `WalboxClient` ready to `run()`.
        """
        checkpoint_store = PostgresCheckpointStore(
            options.dsn,
            consumer_name=options.consumer_name,
        )
        return WalboxClient(options, checkpoint_store)

    @staticmethod
    def build_with_pool(
        options: WalboxOptions,
        pool: ConnectionPool,
    ) -> WalboxClient:
        """Build a client whose checkpoint store reuses your own connection pool.

        `pool` is owned by the caller: opening and closing it (for example
        via `async with AsyncConnectionPool(...) as pool:`) is your
        responsibility, not walbox's. The same pool can also be reused for a
        handler's own downstream writes.

        Returns:
            A `WalboxClient` ready to `run()`.
        """
        checkpoint_store = PostgresCheckpointStore.from_pool(
            pool,
            consumer_name=options.consumer_name,
        )
        return WalboxClient(options, checkpoint_store)
