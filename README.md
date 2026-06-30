# Conveyor

A self-hostable webhook delivery service. Conveyor receives events from your backend, fans them out to registered endpoints, retries on failure with exponential backoff, and enforces per-endpoint rate limits.

## Features

- **Reliable delivery** — Redis Streams as the delivery queue; unACKed messages are reclaimed and retried automatically (XAUTOCLAIM)
- **Exponential backoff** — up to 6 retry attempts with jitter, capped at 1 hour; permanent 4xx errors are dead-lettered immediately
- **Per-endpoint rate limiting** — sliding window (1s) enforced in Redis; throttled deliveries are rescheduled without consuming a retry attempt
- **HMAC signatures** — every delivery is signed with `sha256=<hex>` in the `X-Conveyor-Signature` header
- **Cursor-based pagination** — all list endpoints use keyset pagination, no OFFSET scans

## Architecture

```
Client → POST /v1/events
           │
           ▼
        Postgres (events + deliveries)
        Redis Stream (stream:deliveries)
           │
           ▼
        Worker
        ├── consumer loop   — reads stream, dispatches HTTP
        ├── retry ticker    — every 5s, moves due retries back to stream
        └── reclaim ticker  — every 30s, reclaims stale unACKed messages
```

## Live Demo

```
https://conveyor-api-1inx.onrender.com
```

```bash
# Health check
curl https://conveyor-api-1inx.onrender.com/readyz

# Create a project
curl -X POST https://conveyor-api-1inx.onrender.com/v1/projects \
  -H "Content-Type: application/json" \
  -d '{"name":"my-project"}'
```

## Prerequisites

- Go 1.26+
- Docker (for Postgres + Redis)
- [golang-migrate](https://github.com/golang-migrate/migrate) — `go install -tags 'postgres' github.com/golang-migrate/migrate/v4/cmd/migrate@latest`

## Local Setup

```bash
# 1. Start Postgres + Redis
make up

# 2. Run migrations
make migrate-up

# 3. Start the API server (port 8080)
make run-api

# 4. Start the worker (separate terminal)
make run-worker
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `PORT` | `8080` | API listen port |
| `DATABASE_URL` | `postgres://conveyor:conveyor@localhost:5432/conveyor?sslmode=disable` | Postgres connection string |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection string |
| `API_KEY_PREFIX` | `whk_live_` | Prefix for generated API keys |

## API

All endpoints (except `/v1/projects` and health checks) require an `Authorization: Bearer <api_key>` header.

### Projects

```
POST   /v1/projects           Create a project, returns api_key
GET    /v1/projects/me        Get current project
POST   /v1/projects/me/rotate-key
```

### Endpoints

```
POST   /v1/endpoints
GET    /v1/endpoints
GET    /v1/endpoints/{id}
PATCH  /v1/endpoints/{id}
DELETE /v1/endpoints/{id}
POST   /v1/endpoints/{id}/test   Send a test delivery
```

### Events

```
POST   /v1/events              Ingest an event (triggers delivery fan-out)
GET    /v1/events
GET    /v1/events/{id}
GET    /v1/events/{id}/deliveries
POST   /v1/events/{id}/replay  Re-enqueue all deliveries for this event
```

### Deliveries

```
GET    /v1/deliveries/dead-letter
POST   /v1/deliveries/{id}/replay
```

### Metrics

```
GET    /v1/metrics/summary     Delivery counts by status
```

### Health

```
GET    /healthz    Liveness — always 200 if the process is running
GET    /readyz     Readiness — 200 if Postgres + Redis are reachable, 503 otherwise
```

## Tests

```bash
# Unit tests (no external services needed)
make test

# Integration tests (requires make up + make migrate-up)
make test-integration

# Both
make test-all

# Coverage report (opens coverage.html)
make test-cover
```

## Docker

```bash
# Build both images
make docker-build

# Or individually
docker build --build-arg BINARY=api    -t conveyor-api:latest    .
docker build --build-arg BINARY=worker -t conveyor-worker:latest .

# Run
docker run -e DATABASE_URL=... -e REDIS_URL=... -p 8080:8080 conveyor-api:latest
docker run -e DATABASE_URL=... -e REDIS_URL=...              conveyor-worker:latest
```
