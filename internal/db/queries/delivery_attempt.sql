-- name: CreateDeliveryAttempt :one
INSERT INTO delivery_attempts (delivery_id, attempt_number, http_status, response_body, error)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: ListAttemptsByDelivery :many
SELECT * FROM delivery_attempts
WHERE delivery_id = $1
ORDER BY attempted_at ASC;
