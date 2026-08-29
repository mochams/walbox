# Temporal Workflow Example

This is an illustrative example showing how to start or signal Temporal workflows based on outbox rows. Unlike the [Transactional Outbox](transactional-outbox.md) and [PostgreSQL](postgresql.md) examples, **this code is not backed by a tested script in the repo**. It's provided as a reference implementation pattern; adjust it to your Temporal workflows and configuration.

## The pattern

```python
from temporalio.client import Client
from walbox import ChangeKind, CheckpointHandle, Transaction

client = None

async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue

        row = change.new
        # Use outbox.id as the workflow ID for idempotency
        workflow_id = f"outbox-{row['id']}"

        await client.start_workflow(
            MyWorkflow.run,
            MyWorkflowInput(entity_type=row['entity_type'], payload=row['payload']),
            id=workflow_id,
            task_queue="my-queue",
        )

    await checkpoint.save(tx.commit_lsn)

async def main() -> None:
    global client
    client = await Client.connect("localhost:7233")

    options = WalboxOptions(
        consumer_name="temporal-consumer",
        dsn=dsn,
        slot_name="outbox_slot",
        publication_name="walbox_pub",
    )

    replication_client = WalboxBuilder.build(options)
    # ... signal handlers ...
    await replication_client.run(handle)
```

## Key points

**Workflow ID**: Use `outbox.id` as the workflow ID to ensure each outbox row starts at most one workflow. Temporal's workflow ID uniqueness guarantees idempotency at the workflow level.

**Idempotency**: If the process crashes and restarts, the outbox row is redelivered. When you try to start the same workflow ID again, Temporal returns the existing workflow without duplicating it (if the workflow hasn't completed) or rejects it (if it has), depending on your configuration.

**Signaling workflows**: If you're signaling an existing workflow instead of starting a new one, construct the workflow ID deterministically (e.g., from the entity being modified, not the outbox row ID).

## Full example (outline)

```python
import asyncio
from dataclasses import dataclass
from temporalio.client import Client
from temporalio.workflow import workflow
from temporalio.activity import activity
from walbox import (
    ChangeKind,
    CheckpointHandle,
    Transaction,
    WalboxBuilder,
    WalboxOptions,
)

@dataclass
class OutboxEvent:
    entity_type: str
    entity_id: str
    event_type: str
    payload: dict

@workflow.defn
class ProcessEventWorkflow:
    @workflow.run
    async def run(self, event: OutboxEvent) -> None:
        # Workflow logic here
        await workflow.execute_activity(
            process_event,
            event,
            start_to_close_timeout=timedelta(minutes=10),
        )

@activity.defn
async def process_event(event: OutboxEvent) -> None:
    # Activity logic here
    print(f"Processing {event}")

client = None

async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    for change in tx.changes:
        if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
            continue

        row = change.new
        workflow_id = f"outbox-{row['id']}"

        event = OutboxEvent(
            entity_type=row['entity_type'],
            entity_id=row['entity_id'],
            event_type=row['event_type'],
            payload=row['payload']
        )

        await client.start_workflow(
            ProcessEventWorkflow.run,
            event,
            id=workflow_id,
            task_queue="default",
        )

    await checkpoint.save(tx.commit_lsn)

async def main() -> None:
    global client
    client = await Client.connect("localhost:7233")

    options = WalboxOptions(
        consumer_name="temporal-consumer",
        dsn=dsn,
        slot_name="outbox_slot",
        publication_name="walbox_pub",
    )

    replication_client = WalboxBuilder.build(options)
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, replication_client.close)

    await replication_client.run(handle)

if __name__ == "__main__":
    asyncio.run(main())
```

## Dependencies

```bash
pip install walbox temporalio
```

## See also

- [Transactional Outbox](transactional-outbox.md) for the external system pattern and at-least-once semantics
- [Delivery Guarantees](../production/delivery-guarantees.md) for understanding redelivery
- [Temporal Python SDK](https://docs.temporal.io/develop/python/) for workflow and activity patterns
