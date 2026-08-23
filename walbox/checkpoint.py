"""CheckpointStore implementations for durably tracking replay position."""

import asyncio
import contextlib
import logging
import os
import time
from collections.abc import AsyncGenerator
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from typing import Any
from typing import Protocol

import psycopg
from psycopg import AsyncConnection
from psycopg import sql

logger = logging.getLogger("walbox.checkpoint")

_Acquire = Callable[[], AbstractAsyncContextManager[AsyncConnection[Any]]]


class ConnectionPool(Protocol):
    """Structural shape of a Postgres connection pool.

    Matches `psycopg_pool.AsyncConnectionPool` (an async context manager
    that checks out a connection and returns it to the pool on exit), but
    is never imported from `psycopg_pool` -- any object shaped like this
    works, so accepting one via `PostgresCheckpointStore.from_pool` doesn't
    add a dependency beyond `psycopg` itself.
    """

    def connection(self) -> AbstractAsyncContextManager[AsyncConnection[Any]]:
        """Check out a connection, returning it to the pool on block exit."""
        ...


@contextlib.asynccontextmanager
async def _connect(dsn: str) -> AsyncGenerator[AsyncConnection[Any]]:
    async with await psycopg.AsyncConnection.connect(dsn) as conn:
        yield conn


class FileCheckpointStore:
    """A crash-safe, disk-backed `CheckpointStore`.

    Uses the standard write-to-temp-file, fsync, atomic-rename pattern: a
    failure at any point during `save` leaves the previously-durable
    checkpoint (if any) intact, never a half-written or corrupted one.
    """

    def __init__(self, path: str | Path) -> None:
        """Initialize with the path the checkpoint LSN is persisted to."""
        self._path = Path(path)

    async def load(self) -> int | None:
        """Return the last durably saved LSN, or None if the file doesn't exist yet."""
        started_at = time.monotonic()
        lsn = await asyncio.to_thread(self._load_sync)
        logger.debug(
            "checkpoint load completed in %.6fs, lsn=%s",
            time.monotonic() - started_at,
            lsn,
            extra={"lsn": lsn},
        )
        return lsn

    def _load_sync(self) -> int | None:
        try:
            text = self._path.read_text()
        except FileNotFoundError:
            return None
        return int(text.strip())

    async def save(
        self,
        lsn: int,
        *,
        connection: AsyncConnection[Any] | None = None,
    ) -> None:
        """Durably persist `lsn`.

        `connection` is accepted (to satisfy the `CheckpointStore` Protocol)
        and ignored -- a plain file can never join a Postgres transaction.
        """
        started_at = time.monotonic()
        await asyncio.to_thread(self._save_sync, lsn)
        logger.debug(
            "checkpoint save completed in %.6fs, lsn=%s",
            time.monotonic() - started_at,
            lsn,
            extra={"lsn": lsn},
        )

    def _save_sync(self, lsn: int) -> None:
        tmp_path = self._path.with_name(self._path.name + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            f.write(str(lsn))
            f.flush()
            os.fsync(f.fileno())
        # Atomic on POSIX: readers never see a half-written file.
        tmp_path.replace(self._path)
        dir_fd = os.open(self._path.parent, os.O_RDONLY)
        try:
            # Durably persist the rename itself, not just the new file's bytes.
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)


class PostgresCheckpointStore:
    """A `CheckpointStore` backed by a row in a Postgres table.

    Its entire reason to exist is that `save` can join a *caller-supplied*
    connection's transaction (via `connection=`) instead of always opening
    its own -- letting an application commit its own sink write and the
    checkpoint update atomically in one transaction, something a
    `FileCheckpointStore` can never do.

    `load()` and `save()` without `connection=` open one ad hoc connection
    per call by default -- fine for checkpointing's inherently low call
    volume (once per transaction at most). A throughput-sensitive consumer
    that finds this overhead worth avoiding can build via `from_pool`
    instead, which reuses a connection pool the application already manages.
    """

    def __init__(
        self,
        dsn: str,
        *,
        consumer_name: str,
        table: str = "walbox_checkpoint",
    ) -> None:
        """Initialize with the DSN to connect with and the consumer to track.

        `table` is only ever a trusted, developer-supplied identifier (never
        end-user input), so it's safely composed into SQL via
        `psycopg.sql.Identifier` (imported here as `sql.Identifier`) rather
        than passed as a bind parameter -- Postgres doesn't allow
        parameterizing table names.
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

        `pool` is never connected to or closed here -- it's the
        application's, created and owned outside this store, exactly like
        `dsn` is just a string the default constructor doesn't own either.
        This only changes where connections for `load()` and connection-less
        `save()` calls come from; `save(lsn, connection=conn)`'s
        same-transaction pattern is untouched; it already uses whatever
        connection the caller passes in, pool or not.

        Returns:
            A `PostgresCheckpointStore` backed by `pool`.
        """
        store = cls.__new__(cls)
        store._acquire = pool.connection  # ruff: ignore[private-member-access] -- alternate constructor
        store._configure(consumer_name=consumer_name, table=table)  # ruff: ignore[private-member-access]
        return store

    def _configure(self, *, consumer_name: str, table: str) -> None:
        self._consumer_name = consumer_name
        self._table = sql.Identifier(table)
        self._schema_ready = False

    async def load(self) -> int | None:
        """Return the last durably saved LSN, or None if this consumer has none yet."""
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

        If `connection` is given, the upsert (and, on first use, the backing
        table's creation) is executed on it and left uncommitted -- the
        caller owns the transaction boundary, so this can become durable
        atomically together with whatever else the caller writes on that
        same connection. Without `connection`, this acquires its own
        connection (a fresh one, or one from `from_pool`'s pool) and commits
        immediately.
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

        Left uncommitted deliberately: `CREATE TABLE IF NOT EXISTS` is
        transactional in Postgres, so it's safe to run on a caller-supplied
        `save(connection=...)` connection and let the caller's own commit
        (or the own-connection paths' `async with`/explicit commit) persist
        it -- committing here would durably commit the caller's in-progress
        transaction early, breaking the same-transaction guarantee `save`'s
        `connection=` parameter exists to provide.
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
