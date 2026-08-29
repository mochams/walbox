"""Standalone subprocess harness for real process-kill integration tests.

Not a test module (see `test_kill_recovery.py`): run as a genuine OS
subprocess so it can be sent an actual, uncatchable `SIGKILL` -- something no
in-process fake or `pg_terminate_backend` can simulate. Configuration comes
from environment variables; progress is reported as one line per event,
flushed immediately, on stdout, since a killed process leaves no other way
for the parent test to observe what it durably completed before dying.

Environment variables:
    KILL_HARNESS_DSN: PostgreSQL connection string.
    KILL_HARNESS_SLOT_NAME: replication slot name.
    KILL_HARNESS_CONSUMER_NAME: checkpoint consumer name.
    KILL_HARNESS_BLOCK_ENTITY_ID: if set, the handler hangs forever (never
        calls checkpoint.save()) the first time it sees this entity_id,
        simulating a crash mid-processing, before the checkpoint.

Stdout lines:
    BLOCKING <entity_id>: about to hang forever on the blocked entity.
    CHECKPOINTED <entity_id> <commit_lsn>: checkpoint.save() returned.
"""

import asyncio
import os

from walbox import CheckpointHandle
from walbox import Transaction
from walbox import Walbox
from walbox import WalboxOptions


async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    entity_id = tx.changes[0].new["entity_id"]
    if entity_id == os.environ.get("KILL_HARNESS_BLOCK_ENTITY_ID"):
        print(f"BLOCKING {entity_id}", flush=True)
        await asyncio.Event().wait()  # never resolves; killed before returning

    await checkpoint.save(tx.commit_lsn)
    print(f"CHECKPOINTED {entity_id} {tx.commit_lsn}", flush=True)


async def main() -> None:
    options = WalboxOptions(
        consumer_name=os.environ["KILL_HARNESS_CONSUMER_NAME"],
        dsn=os.environ["KILL_HARNESS_DSN"],
        slot_name=os.environ["KILL_HARNESS_SLOT_NAME"],
        publication_name="walbox_pub",
        status_interval=1,
    )
    client = Walbox.build(options)
    await client.run(handle)


if __name__ == "__main__":
    asyncio.run(main())
