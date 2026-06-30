-- name: CreateProject :one
INSERT INTO projects (name, api_key)
VALUES ($1, $2)
RETURNING *;

-- name: GetProjectByAPIKey :one
SELECT * FROM projects
WHERE api_key = $1
LIMIT 1;

-- name: GetProjectByID :one
SELECT * FROM projects
WHERE id = $1
LIMIT 1;

-- name: UpdateProjectAPIKey :one
UPDATE projects
SET api_key = $2
WHERE id = $1
RETURNING *;
