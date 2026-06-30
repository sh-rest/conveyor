//go:build integration

package testutil

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// NewRedis creates a Redis client for integration tests and registers cleanup.
func NewRedis(t *testing.T) *redis.Client {
	t.Helper()
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	require.NoError(t, rdb.Ping(context.Background()).Err(), "testutil.NewRedis: ping redis")
	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// FlushRedis flushes the current Redis DB. Call at the start of each test for isolation.
func FlushRedis(t *testing.T, rdb *redis.Client) {
	t.Helper()
	require.NoError(t, rdb.FlushDB(context.Background()).Err())
}
