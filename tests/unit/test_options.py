"""Unit tests for `WalboxOptions` construction-time validation."""

import pytest

from walbox.abc import WalboxOptions


def _walbox_kwargs(**overrides: object) -> dict[str, object]:
    return {
        "consumer_name": "test-consumer",
        "dsn": "postgresql://example",
        "slot_name": "test_slot",
        "publication_name": "test_pub",
        **overrides,
    }


def test_walbox_options_construct_cleanly_with_valid_values():
    options = WalboxOptions(**_walbox_kwargs())

    assert options.consumer_name == "test-consumer"
    assert options.max_pending_transactions == 100
    assert options.status_interval == 10


@pytest.mark.parametrize(
    "field_name",
    ["consumer_name", "dsn", "slot_name", "publication_name"],
)
@pytest.mark.parametrize("blank_value", ["", "   "])
def test_walbox_options_rejects_blank_required_strings(field_name, blank_value):
    with pytest.raises(ValueError, match=field_name):
        WalboxOptions(**_walbox_kwargs(**{field_name: blank_value}))


@pytest.mark.parametrize("value", [0, -1])
def test_walbox_options_rejects_non_positive_max_pending_transactions(value):
    with pytest.raises(ValueError, match="max_pending_transactions"):
        WalboxOptions(**_walbox_kwargs(max_pending_transactions=value))


@pytest.mark.parametrize("value", [0, -1])
def test_walbox_options_rejects_non_positive_status_interval(value):
    with pytest.raises(ValueError, match="status_interval"):
        WalboxOptions(**_walbox_kwargs(status_interval=value))
