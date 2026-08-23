"""Structured exception hierarchy for walbox.

`WalboxError` and its subclasses carry an `ErrorContext` payload so callers
get replication-specific context (slot, publication, LSN, xid, relation,
protocol message type) instead of ad hoc `ValueError`/`RuntimeError` text.
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ErrorContext:
    """Immutable bundle of replication context fields, all optional.

    No single error site knows every field, a lower layer raises with what
    it knows and a higher layer fills in the rest via `WalboxError.enrich`.
    """

    slot: str | None = None
    publication: str | None = None
    lsn: int | None = None
    xid: int | None = None
    relation: str | None = None
    message_type: str | None = None


def _render_context(context: ErrorContext) -> str:
    fields = {
        name: value
        for name, value in (
            ("slot", context.slot),
            ("publication", context.publication),
            ("lsn", context.lsn),
            ("xid", context.xid),
            ("relation", context.relation),
            ("message_type", context.message_type),
        )
        if value is not None
    }
    if not fields:
        return ""
    pairs = ", ".join(f"{name}={value!r}" for name, value in fields.items())
    return f" [{pairs}]"


class WalboxError(Exception):
    """Base exception for every error walbox raises, carrying an `ErrorContext`."""

    def __init__(self, message: str, *, context: ErrorContext | None = None) -> None:
        """Initialize with a message and optional starting context."""
        super().__init__(message)
        self.message = message
        self.context = context if context is not None else ErrorContext()

    def enrich(
        self,
        *,
        slot: str | None = None,
        publication: str | None = None,
        lsn: int | None = None,
        xid: int | None = None,
        relation: str | None = None,
        message_type: str | None = None,
    ) -> None:
        """Merge additional context in place as the exception propagates.

        Layers that know more than the original raise site did add to the
        context as it crosses boundaries, e.g. protocol.py raises a
        DecodeError knowing only the message type, and transaction.py adds
        the xid before letting it propagate further. Only non-None arguments
        overwrite; fields already set are preserved unless explicitly
        overridden.
        """
        self.context = ErrorContext(
            slot=slot if slot is not None else self.context.slot,
            publication=(
                publication if publication is not None else self.context.publication
            ),
            lsn=lsn if lsn is not None else self.context.lsn,
            xid=xid if xid is not None else self.context.xid,
            relation=relation if relation is not None else self.context.relation,
            message_type=(
                message_type if message_type is not None else self.context.message_type
            ),
        )

    def __str__(self) -> str:
        """Render as the message, plus any set context fields in brackets.

        Returns:
            The message, followed by a bracketed list of the context fields
            that are set, if any.
        """
        rendered = _render_context(self.context)
        return f"{self.message}{rendered}" if rendered else self.message


class ProtocolError(WalboxError):
    """The replication byte stream or message sequence violated protocol expectations.

    Covers malformed framing, or messages arriving out of the order the
    protocol guarantees (e.g. a second Begin before a Commit).
    """


class DecodeError(WalboxError):
    """A pgoutput message's bytes could not be decoded into its expected shape."""


class ReplicationConnectionError(WalboxError):
    """The replication connection could not be established, or was lost."""


class CheckpointError(WalboxError):
    """A CheckpointStore failed to load or durably save a replay position."""
