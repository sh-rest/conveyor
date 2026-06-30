CREATE TABLE delivery_attempts (
    id             UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    delivery_id    UUID        NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
    attempt_number INT         NOT NULL,
    http_status    INT,
    response_body  TEXT,
    error          TEXT,
    attempted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_delivery_attempts_delivery_id
    ON delivery_attempts(delivery_id, attempted_at DESC);
