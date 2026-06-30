package handler

import (
	"encoding/json"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

type HealthHandler struct {
	pool *pgxpool.Pool
	rdb  *redis.Client
}

func NewHealthHandler(pool *pgxpool.Pool, rdb *redis.Client) *HealthHandler {
	return &HealthHandler{pool: pool, rdb: rdb}
}

func (h *HealthHandler) Readyz(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	checks := map[string]string{}
	ok := true

	if err := h.pool.Ping(ctx); err != nil {
		checks["postgres"] = err.Error()
		ok = false
	}
	if err := h.rdb.Ping(ctx).Err(); err != nil {
		checks["redis"] = err.Error()
		ok = false
	}

	if ok {
		w.WriteHeader(http.StatusOK)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	json.NewEncoder(w).Encode(map[string]any{"checks": checks})
}
