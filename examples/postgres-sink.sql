-- The downstream Postgres sink this example writes to: a projection built
-- from published_table inserts, entirely separate from the table itself.
CREATE TABLE published_table_projection (
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
