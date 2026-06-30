-- name: CreateDelivery :one
INSERT INTO deliveries (event_id, endpoint_id, status)
VALUES ($1, $2, 'pending')
RETURNING *;

-- name: GetDeliveryByID :one
SELECT * FROM deliveries WHERE id = $1 LIMIT 1;

-- name: GetDeliveryByEventAndEndpoint :one
SELECT * FROM deliveries
WHERE event_id = $1 AND endpoint_id = $2
LIMIT 1;

-- name: ListDeliveriesByEvent :many
SELECT * FROM deliveries
WHERE event_id = $1
ORDER BY created_at DESC;

-- name: ListDeliveriesByEndpoint :many
SELECT * FROM deliveries
WHERE endpoint_id = $1
ORDER BY created_at DESC
LIMIT $2;

-- name: ListDeadLettered :many
SELECT d.*, e.event_type, e.payload
FROM deliveries d
JOIN events e ON e.id = d.event_id
WHERE d.status = 'dead_lettered'
    AND e.project_id = $1
ORDER BY d.updated_at DESC
LIMIT $2;

-- name: UpdateDeliverySuccess :one
UPDATE deliveries
SET
    status           = 'delivered',
    attempt_number   = attempt_number + 1,
    last_http_status = $2,
    delivered_at     = NOW(),
    updated_at       = NOW()
WHERE id = $1
RETURNING *;

-- name: UpdateDeliveryFailed :one
UPDATE deliveries
SET
    status              = $2,
    attempt_number      = attempt_number + 1,
    last_http_status    = $3,
    last_response_body  = $4,
    last_error          = $5,
    next_attempt_at     = $6,
    updated_at          = NOW()
WHERE id = $1
RETURNING *;

-- name: ResetDelivery :one
UPDATE deliveries
SET
    status             = 'pending',
    attempt_number     = 0,
    next_attempt_at    = NOW(),
    last_http_status   = NULL,
    last_response_body = NULL,
    last_error         = NULL,
    delivered_at       = NULL,
    updated_at         = NOW()
WHERE id = $1
RETURNING *;

-- name: UpdateDeliveryInFlight :one
UPDATE deliveries
SET status = 'in_flight', updated_at = NOW()
WHERE id = $1
RETURNING *;

-- name: GetDeliveryWithDetails :one
SELECT
    d.id, d.attempt_number, d.status,
    e.id AS event_id, e.payload, e.event_type,
    ep.id AS endpoint_id, ep.url, ep.secret,
    ep.timeout_ms, ep.is_active, ep.rate_limit_rps, ep.max_retries
FROM deliveries d
JOIN events e    ON e.id  = d.event_id
JOIN endpoints ep ON ep.id = d.endpoint_id
WHERE d.id = $1
LIMIT 1;

-- name: ListDueRetries :many
SELECT * FROM deliveries
WHERE status = 'pending'
    AND next_attempt_at <= NOW()
ORDER BY next_attempt_at ASC
LIMIT $1;

-- name: DeliveryMetricsSummary :one
SELECT
    COUNT(*) FILTER (WHERE status = 'delivered')     AS delivered,
    COUNT(*) FILTER (WHERE status = 'failed')        AS failed,
    COUNT(*) FILTER (WHERE status = 'dead_lettered') AS dead_lettered,
    COUNT(*) FILTER (WHERE status = 'pending')       AS pending,
    COUNT(*)                                          AS total
FROM deliveries d
JOIN events e ON e.id = d.event_id
WHERE e.project_id = $1
    AND d.created_at >= NOW() - ($2 || ' hours')::interval;
