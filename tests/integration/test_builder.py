"""Integration tests for `Walbox` against a real Postgres.

`tests/unit/test_builder.py` uses a fake pool double for `build_with_pool()`;
this instead proves the checkpoint store `Walbox` wires up actually
round-trips a real `load()`/`save()` against a live Postgres, including
through a real, caller-managed `psycopg_pool.AsyncConnectionPool`.
"""

import uuid

import pytest
from psycopg_pool import AsyncConnectionPool

from walbox.abc import WalboxOptions
from walbox.builder import Walbox

pytestmark = pytest.mark.postgres


def _options(postgres_dsn: str) -> WalboxOptions:
    return WalboxOptions(
        consumer_name=f"consumer_{uuid.uuid4().hex}",
        dsn=postgres_dsn,
        slot_name=f"slot_{uuid.uuid4().hex}",
        publication_name="walbox_pub",
    )


async def test_build_checkpoint_store_round_trips_against_real_postgres(postgres_dsn):
    client = Walbox.build(_options(postgres_dsn))

    assert await client.checkpoint_store.load() is None
    await client.checkpoint_store.save(42)

    assert await client.checkpoint_store.load() == 42


async def test_build_with_pool_checkpoint_store_round_trips_against_real_postgres(
    postgres_dsn,
):
    async with AsyncConnectionPool(postgres_dsn, min_size=1, max_size=5) as pool:
        client = Walbox.build_with_pool(_options(postgres_dsn), pool)

        assert await client.checkpoint_store.load() is None
        await client.checkpoint_store.save(99)

        assert await client.checkpoint_store.load() == 99
