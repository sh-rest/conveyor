//go:build integration

package testutil

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/sh-rest/conveyor/internal/models"
	"github.com/stretchr/testify/require"
)

const TestDBURL = "postgres://conveyor:conveyor@localhost:5432/conveyor?sslmode=disable"

// NewPool opens a pgxpool for integration tests and registers cleanup.
func NewPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, TestDBURL)
	require.NoError(t, err, "testutil.NewPool: connect to postgres")
	require.NoError(t, pool.Ping(ctx), "testutil.NewPool: ping postgres")
	t.Cleanup(pool.Close)
	return pool
}

// NewTestTx begins a transaction and returns a *models.Queries backed by it.
// The transaction is rolled back automatically via t.Cleanup — no data persists.
func NewTestTx(t *testing.T, pool *pgxpool.Pool) (pgx.Tx, *models.Queries) {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	require.NoError(t, err, "testutil.NewTestTx: begin transaction")
	t.Cleanup(func() {
		_ = tx.Rollback(context.Background())
	})
	return tx, models.New(tx)
}
