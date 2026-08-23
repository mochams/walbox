-- Schema for the outbox table
-- id: unique identifier for each outbox entry
-- entity_type: type of the entity that generated the event (e.g., "user", "order", etc.)
-- entity_id: unique identifier for the entity that generated the event (e.g., "user-123", "order-456", etc.)
-- event_type: type of the event (e.g., "user_created", "order_placed", etc.)
-- payload: JSONB data containing the event details
-- created_at: timestamp when the outbox entry was created
CREATE TABLE outbox (
    id bigserial PRIMARY KEY,
    entity_type text NOT NULL,
    entity_id text NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);


-- Publication
 CREATE PUBLICATION walbox_pub FOR TABLE outbox;
