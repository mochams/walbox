# HTTP Webhooks Example

This is an illustrative example showing how to send outbox rows to an HTTP webhook. Unlike the [Transactional Outbox](transactional-outbox.md) and [PostgreSQL](postgresql.md) examples, **this code is not backed by a tested script in the repo**. It's provided as a reference implementation pattern; adjust it to your webhook URLs and retry logic.

## The pattern

```python
import aiohttp
from walbox import ChangeKind, CheckpointHandle, Transaction

async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    async with aiohttp.ClientSession() as session:
        for change in tx.changes:
            if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
                continue

            row = change.new
            # POST to the webhook with idempotency key
            async with session.post(
                "https://example.com/events",
                json=row,
                headers={"Idempotency-Key": f"outbox-{row['id']}"},
            ) as resp:
                if resp.status >= 400:
                    raise Exception(f"Webhook failed: {resp.status}")

    await checkpoint.save(tx.commit_lsn)
```

## Key points

**Idempotency key**: Use the `Idempotency-Key` header with `outbox.id` so the webhook recipient can deduplicate. This is a common convention (e.g., Stripe, GitHub).

**Retry logic**: If the webhook is down or slow, you have two options:

1. Let the exception propagate, which ends the walbox process. Your supervisor restarts it, and the row is redelivered.
2. Implement retry logic in your handler (exponential backoff, max retries), then let the exception propagate if retries are exhausted.

**Timeouts**: Set a reasonable timeout so a hung webhook doesn't block forever. If the timeout expires, treat it as an error and either retry or propagate the exception.

## Full example (outline)

```python
import asyncio
import json
import logging
import aiohttp
from walbox import (
    ChangeKind,
    CheckpointHandle,
    Transaction,
    WalboxBuilder,
    WalboxOptions,
)

logger = logging.getLogger("webhooks_example")

async def send_webhook_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    row: dict,
    max_retries: int = 3,
) -> None:
    idempotency_key = f"outbox-{row['id']}"

    for attempt in range(max_retries):
        try:
            async with session.post(
                url,
                json=row,
                headers={"Idempotency-Key": idempotency_key},
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    logger.info(f"Webhook succeeded for {idempotency_key}")
                    return
                elif resp.status >= 500:
                    # Server error, retry
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2 ** attempt)  # Exponential backoff
                        continue
                    raise Exception(f"Webhook failed with status {resp.status}")
                else:
                    # Client error, don't retry
                    raise Exception(f"Webhook failed with status {resp.status}")
        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                await asyncio.sleep(2 ** attempt)
                continue
            raise

async def handle(tx: Transaction, checkpoint: CheckpointHandle) -> None:
    async with aiohttp.ClientSession() as session:
        for change in tx.changes:
            if change.table != "public.outbox" or change.kind != ChangeKind.INSERT:
                continue

            row = change.new
            await send_webhook_with_retry(
                session,
                "https://example.com/events",
                row,
                max_retries=3,
            )

    await checkpoint.save(tx.commit_lsn)

async def main() -> None:
    options = WalboxOptions(
        consumer_name="webhooks-consumer",
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

## Webhook endpoint design

When building the webhook endpoint that receives these events, follow these practices:

1. **Accept the Idempotency-Key header** and track processed keys in your database to deduplicate.
2. **Return 2xx on success**, anything else is treated as a failure by the handler.
3. **Be idempotent**: the same request with the same Idempotency-Key should produce the same result, even if called multiple times.
4. **Use a request ID** for logging and debugging, ideally from the Idempotency-Key or a separate header.

Example webhook endpoint (in your downstream service):

```python
# Flask example
from flask import request, jsonify

seen_idempotency_keys = {}  # In production, use a database

@app.route("/events", methods=["POST"])
def receive_event():
    idempotency_key = request.headers.get("Idempotency-Key")

    if idempotency_key in seen_idempotency_keys:
        # Already processed, return the same response
        return jsonify(seen_idempotency_keys[idempotency_key])

    # Process the event
    event = request.json
    # ... your application logic ...

    response = {"status": "ok", "id": event["id"]}
    seen_idempotency_keys[idempotency_key] = response
    return jsonify(response)
```

## Dependencies

```bash
pip install walbox aiohttp
```

## See also

- [Transactional Outbox](transactional-outbox.md) for the external system pattern
- [Delivery Guarantees](../production/delivery-guarantees.md) for at-least-once semantics and redelivery
- Idempotency key standard: [IETF Draft](https://datatracker.ietf.org/doc/html/draft-idempotency-header-last)
