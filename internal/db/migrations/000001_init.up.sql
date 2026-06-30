CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE projects (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name        TEXT NOT NULL,
    api_key     TEXT NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_projects_api_key ON projects(api_key);

CREATE TABLE endpoints (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    url             TEXT NOT NULL,
    description     TEXT,
    secret          TEXT NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    rate_limit_rps  INT NOT NULL DEFAULT 10,
    timeout_ms      INT NOT NULL DEFAULT 5000,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_endpoints_project_id ON endpoints(project_id);
