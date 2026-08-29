"""Integration tests for surviving a genuine, uncatchable process kill.

Every other reconnect/shutdown test either drops the *connection*
(`pg_terminate_backend`, `test_reconnect.py`) while the same Python process
and event loop stay alive to run walbox's own retry logic, or exercises
*graceful* shutdown (`close()`, `test_shutdown.py`), which runs cooperative
cleanup code. Neither proves anything about an actual `SIGKILL`: a real crash
gives the process no chance to run any code at all, and a real recovery
means a brand new OS process, not an in-process fake standing in for one.

These tests run walbox as a genuine subprocess (`_kill_harness.py`), send it
`SIGKILL`, and start a second, independently-cold-started subprocess against
the same slot/consumer afterward, asserting the durable-checkpoint contract
holds across an actual, uncontrolled death and restart.
"""

import asyncio
import contextlib
import os
import sys
import uuid
from pathlib import Path

import pytest
from psycopg import AsyncConnection

pytestmark = pytest.mark.postgres

_HARNESS_PATH = Path(__file__).resolve().parent / "_kill_harness.py"


def _unique_slot_name() -> str:
    return f"slot_{uuid.uuid4().hex}"


def _unique_consumer_name() -> str:
    return f"consumer_{uuid.uuid4().hex}"


def _insert_row(entity_id: str) -> str:
    return (
        "INSERT INTO outbox (entity_type, entity_id, event_type, payload) "
        f"VALUES ('user', '{entity_id}', 'user_created', '{{}}'::jsonb)"
    )


async def _wait_slot_active(dsn: str, slot_name: str, attempts: int = 150) -> None:
    async with await AsyncConnection.connect(dsn, autocommit=True) as conn:
        for _ in range(attempts):
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT active FROM pg_replication_slots WHERE slot_name = %s",
                    (slot_name,),
                )
                row = await cur.fetchone()
            if row is not None and row[0]:
                return
            await asyncio.sleep(0.1)
    pytest.fail(f"slot {slot_name} did not become active in time")


async def _spawn_harness(
    dsn: str,
    slot_name: str,
    consumer_name: str,
    *,
    block_entity_id: str | None = None,
) -> asyncio.subprocess.Process:
    env = dict(os.environ)
    env["KILL_HARNESS_DSN"] = dsn
    env["KILL_HARNESS_SLOT_NAME"] = slot_name
    env["KILL_HARNESS_CONSUMER_NAME"] = consumer_name
    if block_entity_id is None:
        env.pop("KILL_HARNESS_BLOCK_ENTITY_ID", None)
    else:
        env["KILL_HARNESS_BLOCK_ENTITY_ID"] = block_entity_id
    return await asyncio.create_subprocess_exec(
        sys.executable,
        str(_HARNESS_PATH),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )


async def _read_until(
    process: asyncio.subprocess.Process,
    prefix: str,
    timeout: float = 15.0,
) -> str:
    """Read stdout lines until one starts with `prefix`.

    Returns:
        The matching line, stripped of its trailing newline.
    """
    seen: list[str] = []

    async def _read() -> str:
        assert process.stdout is not None
        while True:
            raw = await process.stdout.readline()
            if not raw:
                message = (
                    f"subprocess stdout closed before a line starting "
                    f"{prefix!r}; saw: {seen}"
                )
                raise AssertionError(message)
            line = raw.decode().rstrip("\n")
            seen.append(line)
            if line.startswith(prefix):
                return line

    try:
        return await asyncio.wait_for(_read(), timeout=timeout)
    except TimeoutError as exc:
        message = f"timed out waiting for a line starting {prefix!r}; saw: {seen}"
        raise AssertionError(message) from exc


async def _terminate(process: asyncio.subprocess.Process) -> None:
    """Best-effort cleanup for a harness subprocess a test is done with."""
    if process.returncode is None:
        with contextlib.suppress(ProcessLookupError):
            process.kill()
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(process.wait(), timeout=5.0)


@pytest.mark.timeout(45)
async def test_sigkill_and_cold_restart_resumes_only_the_uncheckpointed_transaction(
    postgres_dsn,
    outbox_table,
):
    slot_name = _unique_slot_name()
    consumer_name = _unique_consumer_name()

    first = await _spawn_harness(postgres_dsn, slot_name, consumer_name)
    try:
        await _wait_slot_active(postgres_dsn, slot_name)

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(_insert_row("tx-a"))
        await _read_until(first, "CHECKPOINTED tx-a ")

        # SIGKILL: uncatchable, no cleanup code in the process runs at all.
        first.kill()
        returncode = await asyncio.wait_for(first.wait(), timeout=10.0)
        assert returncode == -9

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(_insert_row("tx-b"))

        # A genuinely fresh process: new PID, new event loop, no shared
        # Python state with `first` at all.
        second = await _spawn_harness(postgres_dsn, slot_name, consumer_name)
        try:
            await _wait_slot_active(postgres_dsn, slot_name)
            line = await _read_until(second, "CHECKPOINTED", timeout=20.0)
            # Only the transaction `first` never got to see; the one it
            # already durably checkpointed before dying must not recur.
            assert line.startswith("CHECKPOINTED tx-b ")
        finally:
            await _terminate(second)
    finally:
        await _terminate(first)


@pytest.mark.timeout(45)
async def test_sigkill_mid_handler_redelivers_the_uncheckpointed_transaction(
    postgres_dsn,
    outbox_table,
):
    slot_name = _unique_slot_name()
    consumer_name = _unique_consumer_name()

    first = await _spawn_harness(
        postgres_dsn,
        slot_name,
        consumer_name,
        block_entity_id="tx-blocked",
    )
    try:
        await _wait_slot_active(postgres_dsn, slot_name)

        async with await AsyncConnection.connect(postgres_dsn, autocommit=True) as conn:
            await conn.execute(_insert_row("tx-blocked"))
        # Confirms the handler is stuck *before* checkpoint.save(), i.e. this
        # transaction was never durably acknowledged.
        await _read_until(first, "BLOCKING tx-blocked")

        first.kill()
        returncode = await asyncio.wait_for(first.wait(), timeout=10.0)
        assert returncode == -9

        second = await _spawn_harness(postgres_dsn, slot_name, consumer_name)
        try:
            await _wait_slot_active(postgres_dsn, slot_name)
            line = await _read_until(second, "CHECKPOINTED", timeout=20.0)
            assert line.startswith("CHECKPOINTED tx-blocked ")
        finally:
            await _terminate(second)
    finally:
        await _terminate(first)
