-- name: CreateEvent :one
INSERT INTO events (project_id, idempotency_key, event_type, payload, headers)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: GetEventByID :one
SELECT * FROM events
WHERE id = $1 AND project_id = $2
LIMIT 1;

-- name: ListEventsByProject :many
SELECT * FROM events
WHERE project_id = $1
    AND ($2::text = '' OR event_type = $2)
    AND ($3::uuid IS NULL OR id < $3)
ORDER BY ingested_at DESC
LIMIT $4;

-- name: GetEventByIdempotencyKey :one
SELECT * FROM events
WHERE project_id = $1 AND idempotency_key = $2
LIMIT 1;
