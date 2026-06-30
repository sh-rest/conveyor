CREATE TABLE events (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    idempotency_key TEXT,
    event_type      TEXT NOT NULL,
    payload         JSONB NOT NULL,
    headers         JSONB,
    ingested_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_events_idempotency
    ON events(project_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX idx_events_project_type ON events(project_id, event_type);
CREATE INDEX idx_events_ingested_at  ON events(ingested_at DESC);
