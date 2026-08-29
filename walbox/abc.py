"""Shared data types and protocols for walbox."""

import time
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from enum import StrEnum
from typing import Any
from typing import Protocol

from psycopg import AsyncConnection

from walbox.errors import CheckpointError


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

        `connection`, if given, is an already-open Postgres connection or
        transaction the implementation can join instead of opening its own.
        """
        ...


@dataclass(frozen=True, slots=True)
class CheckpointHandle:
    """A `CheckpointStore` bound to one run, handed to every handler call."""

    _store: CheckpointStore
    _on_saved: Callable[[int, float], None] | None = None
    _max_lsn: int | None = None

    async def save(
        self,
        lsn: int,
        *,
        connection: AsyncConnection[Any] | None = None,
    ) -> None:
        """Durably persist `lsn` via the bound `CheckpointStore`.

        `connection`, if given, is an already-open Postgres connection or
        transaction the underlying store can join instead of opening its own.

        Raises:
            CheckpointError: If `lsn` is greater than the commit LSN of the
                transaction this handle was constructed for. Saving a
                checkpoint ahead of what was actually processed risks
                PostgreSQL recycling WAL for data walbox never handled.
        """
        if self._max_lsn is not None and lsn > self._max_lsn:
            message = (
                f"refusing to save checkpoint lsn={lsn}: greater than "
                f"the dispatched transaction's commit_lsn={self._max_lsn}"
            )
            raise CheckpointError(message)
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

    Handed to `WalboxOptions.on_metrics` on the same timer as status
    updates. No historical aggregation (rolling windows, percentiles,
    rates) is done here; that's on the application if it wants one.
    """

    consumer_name: str
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
class WalboxOptions:
    """Configuration for a walbox client, shared by every checkpoint backend.

    `Client` takes this plus a `CheckpointStore` directly;
    `Walbox` constructs the checkpoint store for you.
    """

    consumer_name: str
    dsn: str
    slot_name: str
    publication_name: str

    max_pending_transactions: int = 100
    status_interval: int = 10
    on_metrics: MetricsCallback | None = None

    def __post_init__(self) -> None:
        """Validate required fields, raising `ValueError` if any are invalid.

        Raises:
            ValueError: If any required field is missing or invalid.
        """
        for name, value in (
            ("consumer_name", self.consumer_name),
            ("dsn", self.dsn),
            ("slot_name", self.slot_name),
            ("publication_name", self.publication_name),
        ):
            if not value or not value.strip():
                message = f"{name} must not be blank"
                raise ValueError(message)
        if self.max_pending_transactions <= 0:
            message = "max_pending_transactions must be > 0"
            raise ValueError(message)
        if self.status_interval <= 0:
            message = "status_interval must be > 0"
            raise ValueError(message)
