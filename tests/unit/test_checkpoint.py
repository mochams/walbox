"""Unit tests for `FileCheckpointStore`, using pytest's `tmp_path` -- no Postgres."""

import os
from pathlib import Path

import pytest

from walbox.checkpoint import FileCheckpointStore


async def test_load_returns_none_for_a_missing_file(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "checkpoint")

    assert await store.load() is None


async def test_save_then_load_round_trips_the_value(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "checkpoint")

    await store.save(100)

    assert await store.load() == 100


async def test_save_overwrites_a_previous_value(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "checkpoint")

    await store.save(100)
    await store.save(200)

    assert await store.load() == 200


async def test_save_ignores_the_connection_keyword(tmp_path: Path) -> None:
    store = FileCheckpointStore(tmp_path / "checkpoint")

    await store.save(100, connection=object())

    assert await store.load() == 100


async def test_save_leaves_no_tmp_file_behind_on_success(tmp_path: Path) -> None:
    path = tmp_path / "checkpoint"
    store = FileCheckpointStore(path)

    await store.save(100)

    assert not path.with_name(path.name + ".tmp").exists()


async def test_crash_during_save_leaves_the_previous_checkpoint_intact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The load-bearing crash-safety test.

    A failure injected into the *next* `os.replace` call must propagate
    without corrupting or silently advancing the previously-durable value.
    """
    path = tmp_path / "checkpoint"
    store = FileCheckpointStore(path)
    await store.save(100)

    original_replace = os.replace
    remaining_failures = {"count": 1}

    def _flaky_replace(*args: object, **kwargs: object) -> None:
        if remaining_failures["count"] > 0:
            remaining_failures["count"] -= 1
            injected = "simulated crash during os.replace"
            raise OSError(injected)
        original_replace(*args, **kwargs)

    monkeypatch.setattr(os, "replace", _flaky_replace)

    with pytest.raises(OSError, match="simulated crash during os.replace"):
        await store.save(200)

    assert await store.load() == 100
