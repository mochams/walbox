from dataclasses import FrozenInstanceError

import pytest

from walbox.errors import CheckpointError
from walbox.errors import DecodeError
from walbox.errors import ErrorContext
from walbox.errors import ProtocolError
from walbox.errors import ReplicationConnectionError
from walbox.errors import WalboxError


def test_context_defaults_to_all_none():
    context = ErrorContext()
    assert context.slot is None
    assert context.publication is None
    assert context.lsn is None
    assert context.xid is None
    assert context.relation is None
    assert context.message_type is None


def test_str_returns_message_only_when_context_is_empty():
    assert str(WalboxError("boom")) == "boom"


def test_str_includes_only_the_fields_that_are_set():
    err = WalboxError("boom", context=ErrorContext(slot="s1", lsn=42))
    rendered = str(err)
    assert "slot='s1'" in rendered
    assert "lsn=42" in rendered
    assert "xid" not in rendered
    assert "relation" not in rendered
    assert "publication" not in rendered
    assert "message_type" not in rendered


def test_enrich_adds_new_fields_without_touching_existing_ones():
    err = WalboxError("boom", context=ErrorContext(slot="s1"))
    err.enrich(lsn=100)
    assert err.context.slot == "s1"
    assert err.context.lsn == 100


def test_enrich_ignores_none_arguments():
    err = WalboxError("boom", context=ErrorContext(xid=5))
    err.enrich(xid=None, relation="t")
    assert err.context.xid == 5
    assert err.context.relation == "t"


def test_enrich_can_override_an_existing_field_with_a_real_value():
    err = WalboxError("boom", context=ErrorContext(lsn=1))
    err.enrich(lsn=2)
    assert err.context.lsn == 2


@pytest.mark.parametrize(
    "exc_type",
    [ProtocolError, DecodeError, ReplicationConnectionError, CheckpointError],
)
def test_subclasses_are_walbox_error_instances(exc_type):
    exc = exc_type("boom")
    assert isinstance(exc, WalboxError)

    with pytest.raises(WalboxError):
        raise exc


def test_context_is_immutable():
    context = ErrorContext()
    with pytest.raises(FrozenInstanceError):
        context.lsn = 5
