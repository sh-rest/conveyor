//go:build integration

package ratelimit_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sh-rest/conveyor/internal/ratelimit"
	"github.com/sh-rest/conveyor/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testEndpointID = "ep0000000000000000000000000000a1"

func rlKey(endpointID string) string { return fmt.Sprintf("rl:%s", endpointID) }

func TestAllow_UnderLimit(t *testing.T) {
	rdb := testutil.NewRedis(t)
	testutil.FlushRedis(t, rdb)
	l := ratelimit.NewLimiter(rdb)
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		ok, err := l.Allow(ctx, testEndpointID, fmt.Sprintf("dlv-%d", i), 5)
		require.NoError(t, err)
		assert.True(t, ok, "delivery %d should be allowed", i)
	}
	card := rdb.ZCard(ctx, rlKey(testEndpointID)).Val()
	assert.Equal(t, int64(4), card)
}

func TestAllow_AtLimit(t *testing.T) {
	rdb := testutil.NewRedis(t)
	testutil.FlushRedis(t, rdb)
	l := ratelimit.NewLimiter(rdb)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		ok, err := l.Allow(ctx, testEndpointID, fmt.Sprintf("dlv-%d", i), 3)
		require.NoError(t, err)
		assert.True(t, ok)
	}

	ok, err := l.Allow(ctx, testEndpointID, "dlv-4th", 3)
	require.NoError(t, err)
	assert.False(t, ok, "4th request should be denied when limit is 3")
}

func TestAllow_ExactlyAtLimit(t *testing.T) {
	rdb := testutil.NewRedis(t)
	testutil.FlushRedis(t, rdb)
	l := ratelimit.NewLimiter(rdb)
	ctx := context.Background()

	ok1, err := l.Allow(ctx, testEndpointID, "dlv-1", 1)
	require.NoError(t, err)
	assert.True(t, ok1, "first request must be allowed for limit=1")

	ok2, err := l.Allow(ctx, testEndpointID, "dlv-2", 1)
	require.NoError(t, err)
	assert.False(t, ok2, "second request must be denied for limit=1")
}

func TestAllow_WindowSlide(t *testing.T) {
	rdb := testutil.NewRedis(t)
	testutil.FlushRedis(t, rdb)
	l := ratelimit.NewLimiter(rdb)
	ctx := context.Background()

	// Manually insert a stale entry (> 1s ago)
	staleScore := float64(time.Now().Add(-1001 * time.Millisecond).UnixMilli())
	rdb.ZAdd(ctx, rlKey(testEndpointID), redis.Z{Score: staleScore, Member: "stale-dlv"})

	// With limit=1, the stale entry should be evicted and this request allowed
	ok, err := l.Allow(ctx, testEndpointID, "dlv-fresh", 1)
	require.NoError(t, err)
	assert.True(t, ok, "stale entry should be evicted, request should be allowed")
}

func TestAllow_DifferentEndpoints_Independent(t *testing.T) {
	rdb := testutil.NewRedis(t)
	testutil.FlushRedis(t, rdb)
	l := ratelimit.NewLimiter(rdb)
	ctx := context.Background()

	epA := "epaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
	epB := "epbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb1"

	okA, err := l.Allow(ctx, epA, "dlv-a", 1)
	require.NoError(t, err)
	assert.True(t, okA)

	okB, err := l.Allow(ctx, epB, "dlv-b", 1)
	require.NoError(t, err)
	assert.True(t, okB, "endpoint B should have an independent rate limit window")
}

func TestAllow_TTLSet(t *testing.T) {
	rdb := testutil.NewRedis(t)
	testutil.FlushRedis(t, rdb)
	l := ratelimit.NewLimiter(rdb)
	ctx := context.Background()

	ok, err := l.Allow(ctx, testEndpointID, "dlv-ttl", 10)
	require.NoError(t, err)
	require.True(t, ok)

	ttl, err := rdb.TTL(ctx, rlKey(testEndpointID)).Result()
	require.NoError(t, err)
	assert.GreaterOrEqual(t, ttl, time.Second, "TTL should be at least 1s")
	assert.LessOrEqual(t, ttl, 5*time.Second, "TTL should be at most 5s")
}

func TestAllow_SameDeliveryID_Idempotent(t *testing.T) {
	rdb := testutil.NewRedis(t)
	testutil.FlushRedis(t, rdb)
	l := ratelimit.NewLimiter(rdb)
	ctx := context.Background()

	// Same deliveryID = same ZADD member → second call updates score, doesn't add a new member
	_, err := l.Allow(ctx, testEndpointID, "same-dlv", 2)
	require.NoError(t, err)
	_, err = l.Allow(ctx, testEndpointID, "same-dlv", 2)
	require.NoError(t, err)

	card := rdb.ZCard(ctx, rlKey(testEndpointID)).Val()
	assert.Equal(t, int64(1), card, "same deliveryID should not create duplicate members")
}
