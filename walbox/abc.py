"""Shared data types and protocols for walbox."""

import time
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from enum import StrEnum
from typing import Any
from typing import Protocol

from psycopg import AsyncConnection


class ChangeKind(StrEnum):
    """The kind of row-level change a `ChangeEvent` carries."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    TRUNCATE = "truncate"


@dataclass
class ChangeEvent:
    """A single row-level change within a transaction."""

    kind: ChangeKind
    table: str
    new: dict[str, Any] | None = None
    old: dict[str, Any] | None = None


class CheckpointStore(Protocol):
    """Durably tracks the last replayed LSN so replication can resume after restart."""

    async def load(self) -> int | None:
        """Return the last durably saved LSN, or None if none has been saved yet."""
        ...

    async def save(
        self,
        lsn: int,
        *,
        connection: AsyncConnection[Any] | None = None,
    ) -> None:
        """Durably persist `lsn` as the new replay position.

        Args:
            lsn: The replay position to persist.
            connection: An optional already-open Postgres connection/
                transaction for an implementation to join, instead of
                opening its own.
        """
        ...


@dataclass(frozen=True, slots=True)
class CheckpointHandle:
    """A `CheckpointStore` bound to one run, handed to every handler call."""

    _store: CheckpointStore
    _on_saved: Callable[[int, float], None] | None = None

    async def save(
        self,
        lsn: int,
        *,
        connection: AsyncConnection[Any] | None = None,
    ) -> None:
        """Durably persist `lsn` via the bound `CheckpointStore`.

        Args:
            lsn: The replay position to persist.
            connection: An optional already-open Postgres connection/
                transaction for the underlying store to join, instead of
                opening its own.
        """
        started_at = time.monotonic()
        await self._store.save(lsn, connection=connection)
        if self._on_saved is not None:
            self._on_saved(lsn, time.monotonic() - started_at)


@dataclass(frozen=True, slots=True)
class Transaction:
    """A committed transaction and the row-level changes it contains."""

    xid: int
    commit_lsn: int
    commit_time: int
    changes: list[ChangeEvent] = field(default_factory=list)


@dataclass(frozen=True)
class Metrics:
    """A point-in-time snapshot of replication counters and gauges.

    Handed to `ReplicationOptions.on_metrics` from the same periodic spot
    the status-update timer already fires from -- no historical
    aggregation (rolling windows, percentiles, rates) is done here; that's
    the application's job if it wants one.
    """

    receive_lsn: int
    checkpoint_lsn: int
    replication_lag_bytes: int
    transactions_processed: int
    changes_processed: int
    reconnect_count: int
    last_handler_latency_seconds: float
    queue_depth: int
    last_keepalive_at: float
    last_checkpoint_latency_seconds: float
    transactions_since_checkpoint: int


MetricsCallback = Callable[[Metrics], None]


@dataclass
class ReplicationOptions:
    """Options for replication."""

    consumer_name: str

    dsn: str
    slot_name: str
    publication_name: str
    checkpoint_store: CheckpointStore

    max_pending_transactions: int = 100
    status_interval: int = 10
    on_metrics: MetricsCallback | None = None
