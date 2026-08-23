"""Tests for walbox's frozen public export surface."""

import types

import walbox
from walbox import errors as walbox_errors

_EXPECTED_ALL = {
    "ChangeEvent",
    "ChangeKind",
    "CheckpointStore",
    "FileCheckpointStore",
    "PostgresCheckpointStore",
    "ReplicationClient",
    "ReplicationOptions",
    "Transaction",
    "WalboxError",
}


def test_all_matches_expected_public_surface() -> None:
    assert set(walbox.__all__) == _EXPECTED_ALL


def test_all_has_no_duplicates() -> None:
    assert len(walbox.__all__) == len(set(walbox.__all__))


def test_every_name_in_all_is_actually_importable() -> None:
    for name in walbox.__all__:
        assert hasattr(walbox, name)


def test_no_non_module_attribute_is_exported_but_unlisted() -> None:
    """Every name bound at module scope that isn't a submodule is in `__all__`."""
    bound_names = {
        name
        for name, value in vars(walbox).items()
        if not name.startswith("_") and not isinstance(value, types.ModuleType)
    }
    assert bound_names == set(walbox.__all__)


def test_no_specific_error_subclass_leaks_into_all() -> None:
    error_subclass_names = {
        name
        for name in dir(walbox_errors)
        if isinstance(getattr(walbox_errors, name), type)
        and issubclass(getattr(walbox_errors, name), walbox_errors.WalboxError)
        and name != "WalboxError"
    }
    assert error_subclass_names, "expected at least one specific error subclass"
    assert error_subclass_names.isdisjoint(walbox.__all__)
