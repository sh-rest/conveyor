-- name: CreateEndpoint :one
INSERT INTO endpoints (project_id, url, description, secret, rate_limit_rps, timeout_ms)
VALUES ($1, $2, $3, $4, $5, $6)
RETURNING *;

-- name: GetEndpointByID :one
SELECT * FROM endpoints
WHERE id = $1 AND project_id = $2
LIMIT 1;

-- name: ListEndpointsByProject :many
SELECT * FROM endpoints
WHERE project_id = $1
ORDER BY created_at DESC;

-- name: ListActiveEndpointsByProject :many
SELECT * FROM endpoints
WHERE project_id = $1 AND is_active = TRUE
ORDER BY created_at DESC;

-- name: UpdateEndpoint :one
UPDATE endpoints
SET
    url            = COALESCE(sqlc.narg(url), url),
    description    = COALESCE(sqlc.narg(description), description),
    is_active      = COALESCE(sqlc.narg(is_active), is_active),
    rate_limit_rps = COALESCE(sqlc.narg(rate_limit_rps), rate_limit_rps),
    timeout_ms     = COALESCE(sqlc.narg(timeout_ms), timeout_ms)
WHERE id = $1 AND project_id = $2
RETURNING *;

-- name: DeleteEndpoint :exec
DELETE FROM endpoints
WHERE id = $1 AND project_id = $2;
