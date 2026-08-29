"""CheckpointStore implementations for durably tracking replay position."""

import contextlib
import logging
import time
from collections.abc import AsyncGenerator
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from typing import Any
from typing import Protocol

import psycopg
from psycopg import AsyncConnection
from psycopg import sql

from walbox.errors import CheckpointError

logger = logging.getLogger("walbox.checkpoint")

_Acquire = Callable[[], AbstractAsyncContextManager[AsyncConnection[Any]]]


class ConnectionPool(Protocol):
    """Structural shape of a Postgres connection pool.

    Matches `psycopg_pool.AsyncConnectionPool` (an async context manager
    that checks out a connection and returns it on exit), but is never
    imported from `psycopg_pool`. Any object shaped like this works, so
    `PostgresCheckpointStore.from_pool` adds no dependency beyond `psycopg`.
    """

    def connection(self) -> AbstractAsyncContextManager[AsyncConnection[Any]]:
        """Check out a connection, returning it to the pool on block exit."""
        ...


@contextlib.asynccontextmanager
async def _connect(dsn: str) -> AsyncGenerator[AsyncConnection[Any]]:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        yield conn


class PostgresCheckpointStore:
    """A `CheckpointStore` backed by a row in a Postgres table.

    `save(connection=...)` can join a caller-supplied connection's
    transaction instead of opening its own, letting an application commit
    its own sink write and the checkpoint update atomically in one
    transaction.

    Without `connection=`, `load()` and `save()` open one ad hoc connection
    per call, which is fine for checkpointing's low call volume. Use
    `from_pool` instead to reuse a connection pool the application already
    manages.
    """

    def __init__(
        self,
        dsn: str,
        *,
        consumer_name: str,
        table: str = "walbox_checkpoint",
    ) -> None:
        """Initialize with the DSN to connect with and the consumer to track.

        `table` must be a trusted, developer-supplied identifier, never
        end-user input: Postgres doesn't allow parameterizing table names,
        so it's composed into SQL via `sql.Identifier` instead of a bind
        parameter.
        """
        self._acquire: _Acquire = lambda: _connect(dsn)
        self._configure(consumer_name=consumer_name, table=table)

    @classmethod
    def from_pool(
        cls,
        pool: ConnectionPool,
        *,
        consumer_name: str,
        table: str = "walbox_checkpoint",
    ) -> "PostgresCheckpointStore":
        """Build a store whose ad hoc `load()`/`save()` calls reuse `pool`.

        `pool` is owned by the application; it's never connected to or
        closed here. This only changes where connections for `load()` and
        connection-less `save()` calls come from. `save(lsn, connection=...)`
        already uses whatever connection the caller passes in, pool or not.

        Returns:
            A `PostgresCheckpointStore` backed by `pool`.
        """
        store = cls.__new__(cls)
        store._acquire = pool.connection  # ruff: ignore[private-member-access]: alternate constructor
        store._configure(consumer_name=consumer_name, table=table)  # ruff: ignore[private-member-access]
        return store

    def _configure(self, *, consumer_name: str, table: str) -> None:
        self._consumer_name = consumer_name
        self._table = sql.Identifier(table)
        self._schema_ready = False

    async def load(self) -> int | None:
        """Return the last durably saved LSN, or None if this consumer has none yet.

        Raises:
            CheckpointError: If the saved LSN is negative.
        """
        started_at = time.monotonic()
        async with self._acquire() as conn:
            await self._ensure_schema(conn)
            await conn.commit()
            query = sql.SQL(
                "SELECT lsn FROM {table} WHERE consumer_name = %s",
            ).format(table=self._table)
            cursor = await conn.execute(query, (self._consumer_name,))
            row = await cursor.fetchone()
            lsn = row[0] if row is not None else None
        if lsn is not None and lsn < 0:
            message = (
                f"checkpoint for consumer {self._consumer_name!r} is negative: {lsn}"
            )
            raise CheckpointError(message)
        logger.debug(
            "checkpoint load completed in %.6fs, lsn=%s",
            time.monotonic() - started_at,
            lsn,
            extra={"lsn": lsn},
        )
        return lsn

    async def save(
        self,
        lsn: int,
        *,
        connection: AsyncConnection[Any] | None = None,
    ) -> None:
        """Durably persist `lsn` as the new replay position for this consumer.

        If `connection` is given, the upsert (and, on first use, the
        backing table's creation) runs on it and is left uncommitted, so the
        caller's own commit makes it durable atomically with whatever else
        it writes on that connection. Without `connection`, this acquires
        its own connection and commits immediately.
        """
        started_at = time.monotonic()
        if connection is not None:
            await self._ensure_schema(connection)
            await self._upsert(connection, lsn)
        else:
            async with self._acquire() as conn:
                await self._ensure_schema(conn)
                await self._upsert(conn, lsn)
                await conn.commit()
        logger.debug(
            "checkpoint save completed in %.6fs, lsn=%s",
            time.monotonic() - started_at,
            lsn,
            extra={"lsn": lsn},
        )

    async def _ensure_schema(self, conn: AsyncConnection[Any]) -> None:
        """Create the backing table if needed, without committing.

        `CREATE TABLE IF NOT EXISTS` is transactional in Postgres, so this
        is safe to run on a caller-supplied `save(connection=...)` connection
        and leave for the caller's own commit. Committing here would commit
        the caller's in-progress transaction early, breaking the
        same-transaction guarantee `connection=` exists to provide.
        """
        if self._schema_ready:
            return
        await conn.execute(
            sql.SQL(
                "CREATE TABLE IF NOT EXISTS {table} ("
                "consumer_name TEXT PRIMARY KEY, "
                "lsn BIGINT NOT NULL, "
                "updated_at TIMESTAMPTZ NOT NULL DEFAULT now()"
                ")",
            ).format(table=self._table),
        )
        self._schema_ready = True

    async def _upsert(self, conn: AsyncConnection[Any], lsn: int) -> None:
        await conn.execute(
            sql.SQL(
                "INSERT INTO {table} (consumer_name, lsn) VALUES (%s, %s) "
                "ON CONFLICT (consumer_name) DO UPDATE SET lsn = EXCLUDED.lsn, "
                "updated_at = now()",
            ).format(table=self._table),
            (self._consumer_name, lsn),
        )
