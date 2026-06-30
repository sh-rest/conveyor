//go:build integration

package queue_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sh-rest/conveyor/internal/queue"
	"github.com/sh-rest/conveyor/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func freshScheduler(t *testing.T) (*queue.Scheduler, *redis.Client) {
	t.Helper()
	rdb := testutil.NewRedis(t)
	testutil.FlushRedis(t, rdb)
	ctx := context.Background()
	rdb.XGroupCreateMkStream(ctx, queue.DeliveryStream, queue.ConsumerGroup, "0")
	return queue.NewScheduler(rdb), rdb
}

// --- TestSchedule ---

func TestSchedule_AddsToZSet(t *testing.T) {
	rdb := testutil.NewRedis(t)
	testutil.FlushRedis(t, rdb)
	s := queue.NewScheduler(rdb)
	ctx := context.Background()

	target := time.Now().Add(10 * time.Second)
	require.NoError(t, s.Schedule(ctx, "dlv-1", target))

	score, err := rdb.ZScore(ctx, queue.RetryZSet, "dlv-1").Result()
	require.NoError(t, err)
	assert.InDelta(t, float64(target.UnixMilli()), score, 50, "score should match next_attempt_at in ms")
}

func TestSchedule_OverwritesExisting(t *testing.T) {
	rdb := testutil.NewRedis(t)
	testutil.FlushRedis(t, rdb)
	s := queue.NewScheduler(rdb)
	ctx := context.Background()

	t1 := time.Now().Add(5 * time.Second)
	t2 := time.Now().Add(30 * time.Second)
	require.NoError(t, s.Schedule(ctx, "dlv-1", t1))
	require.NoError(t, s.Schedule(ctx, "dlv-1", t2))

	card := rdb.ZCard(ctx, queue.RetryZSet).Val()
	assert.Equal(t, int64(1), card, "same ID should overwrite, not duplicate")

	score := rdb.ZScore(ctx, queue.RetryZSet, "dlv-1").Val()
	assert.InDelta(t, float64(t2.UnixMilli()), score, 50)
}

// --- TestProcessDue ---

func TestProcessDue_Empty(t *testing.T) {
	s, _ := freshScheduler(t)
	n, err := s.ProcessDue(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestProcessDue_NothingDueYet(t *testing.T) {
	rdb := testutil.NewRedis(t)
	testutil.FlushRedis(t, rdb)
	s := queue.NewScheduler(rdb)
	ctx := context.Background()

	require.NoError(t, s.Schedule(ctx, "dlv-future", time.Now().Add(10*time.Second)))
	n, err := s.ProcessDue(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestProcessDue_MovesOverdueItems(t *testing.T) {
	s, rdb := freshScheduler(t)
	ctx := context.Background()

	past := time.Now().Add(-5 * time.Second)
	for i := 0; i < 3; i++ {
		require.NoError(t, s.Schedule(ctx, fmt.Sprintf("dlv-%d", i), past))
	}

	n, err := s.ProcessDue(ctx)
	require.NoError(t, err)
	assert.Equal(t, 3, n)
	assert.Equal(t, int64(0), rdb.ZCard(ctx, queue.RetryZSet).Val(), "zset should be empty after processing")
	assert.Equal(t, int64(3), rdb.XLen(ctx, queue.DeliveryStream).Val(), "3 entries re-enqueued to stream")
}

func TestProcessDue_OnlyMovesOverdue(t *testing.T) {
	s, rdb := freshScheduler(t)
	ctx := context.Background()

	past := time.Now().Add(-5 * time.Second)
	require.NoError(t, s.Schedule(ctx, "dlv-past-1", past))
	require.NoError(t, s.Schedule(ctx, "dlv-past-2", past))
	require.NoError(t, s.Schedule(ctx, "dlv-future", time.Now().Add(60*time.Second)))

	n, err := s.ProcessDue(ctx)
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	assert.Equal(t, int64(1), rdb.ZCard(ctx, queue.RetryZSet).Val(), "future item must remain")
}

func TestProcessDue_BatchCap(t *testing.T) {
	s, rdb := freshScheduler(t)
	ctx := context.Background()

	past := time.Now().Add(-5 * time.Second)
	for i := 0; i < 101; i++ {
		require.NoError(t, s.Schedule(ctx, fmt.Sprintf("dlv-%d", i), past))
	}

	n, err := s.ProcessDue(ctx)
	require.NoError(t, err)
	assert.Equal(t, 100, n, "batch cap is 100")
	assert.Equal(t, int64(1), rdb.ZCard(ctx, queue.RetryZSet).Val(), "one item remains after batch cap")
}

// --- TestReclaimStale ---

func TestReclaimStale_Empty(t *testing.T) {
	s, _ := freshScheduler(t)
	n, err := s.ReclaimStale(context.Background(), 0)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
}

func TestReclaimStale_ReclaimesPendingMessages(t *testing.T) {
	_, rdb := freshScheduler(t)
	ctx := context.Background()

	// Add a message to the stream
	rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: queue.DeliveryStream,
		Values: map[string]any{"delivery_id": "dlv-stale"},
	})

	// Read with a dummy consumer — creates a PEL entry but does NOT ACK
	rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    queue.ConsumerGroup,
		Consumer: "dummy-consumer",
		Streams:  []string{queue.DeliveryStream, ">"},
		Count:    1,
	})

	// Confirm there is 1 pending entry before reclaim
	pending := rdb.XPending(ctx, queue.DeliveryStream, queue.ConsumerGroup).Val()
	require.Equal(t, int64(1), pending.Count, "setup: should have 1 pending message")

	// Reclaim with minIdle=0 (claim immediately regardless of idle time)
	s := queue.NewScheduler(rdb)
	n, err := s.ReclaimStale(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "one stale message should be reclaimed and re-enqueued")

	// PEL should now be empty (original entry was ACKed by ReclaimStale)
	pending2 := rdb.XPending(ctx, queue.DeliveryStream, queue.ConsumerGroup).Val()
	assert.Equal(t, int64(0), pending2.Count, "PEL should be empty after reclaim")
}
