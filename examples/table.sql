-- Table used in examples to demonstrate WALBOX functionality.
CREATE TABLE published_table (
    id          BIGSERIAL PRIMARY KEY,
    entity_type TEXT NOT NULL,
    entity_id   TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Publication of the table above.
CREATE PUBLICATION walbox_pub FOR TABLE published_table;
