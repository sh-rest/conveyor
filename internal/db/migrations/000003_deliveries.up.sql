CREATE TYPE delivery_status AS ENUM (
    'pending',
    'in_flight',
    'delivered',
    'failed',
    'dead_lettered'
);

CREATE TABLE deliveries (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id            UUID NOT NULL REFERENCES events(id) ON DELETE CASCADE,
    endpoint_id         UUID NOT NULL REFERENCES endpoints(id) ON DELETE CASCADE,
    status              delivery_status NOT NULL DEFAULT 'pending',
    attempt_number      INT NOT NULL DEFAULT 0,
    next_attempt_at     TIMESTAMPTZ,
    last_http_status    INT,
    last_response_body  TEXT,
    last_error          TEXT,
    delivered_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_deliveries_unique
    ON deliveries(event_id, endpoint_id);

CREATE INDEX idx_deliveries_status       ON deliveries(status);
CREATE INDEX idx_deliveries_endpoint_id  ON deliveries(endpoint_id);

CREATE INDEX idx_deliveries_next_attempt
    ON deliveries(next_attempt_at)
    WHERE status = 'pending';
