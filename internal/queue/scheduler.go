package queue

import (
	"context"
	"log/slog"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const RetryZSet = "zset:retry"

type Scheduler struct {
	rdb *redis.Client
}

func NewScheduler(rdb *redis.Client) *Scheduler {
	return &Scheduler{rdb: rdb}
}

// Schedule adds a delivery to the retry sorted set with score = next_attempt Unix ms.
// The scheduler goroutine polls this set and re-enqueues due items to the stream.
func (s *Scheduler) Schedule(ctx context.Context, deliveryID string, at time.Time) error {
	return s.rdb.ZAdd(ctx, RetryZSet, redis.Z{
		Score:  float64(at.UnixMilli()),
		Member: deliveryID,
	}).Err()
}

// ProcessDue finds all deliveries due for retry (score <= now), re-enqueues them
// to the stream via a pipeline, then removes them from the sorted set.
// Returns the number of items re-enqueued.
func (s *Scheduler) ProcessDue(ctx context.Context) (int, error) {
	nowMs := strconv.FormatInt(time.Now().UnixMilli(), 10)

	members, err := s.rdb.ZRangeByScore(ctx, RetryZSet, &redis.ZRangeBy{
		Min:   "0",
		Max:   nowMs,
		Count: 100,
	}).Result()
	if err != nil || len(members) == 0 {
		return 0, err
	}

	pipe := s.rdb.Pipeline()
	for _, id := range members {
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: DeliveryStream,
			Values: map[string]any{"delivery_id": id},
		})
	}
	// Convert []string to []any for ZRem
	toRemove := make([]any, len(members))
	for i, m := range members {
		toRemove[i] = m
	}
	pipe.ZRem(ctx, RetryZSet, toRemove...)

	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return len(members), nil
}

// ReclaimStale uses XAUTOCLAIM to reassign PEL entries that have been idle longer
// than minIdle. This recovers deliveries whose worker crashed before ACKing.
// Returns the number of messages reclaimed and re-enqueued.
func (s *Scheduler) ReclaimStale(ctx context.Context, minIdle time.Duration) (int, error) {
	result, _, err := s.rdb.XAutoClaim(ctx, &redis.XAutoClaimArgs{
		Stream:   DeliveryStream,
		Group:    ConsumerGroup,
		Consumer: "scheduler-reclaimer",
		MinIdle:  minIdle,
		Start:    "0-0",
		Count:    100,
	}).Result()
	if err != nil {
		return 0, err
	}

	if len(result) == 0 {
		return 0, nil
	}

	pipe := s.rdb.Pipeline()
	for _, msg := range result {
		deliveryID, _ := msg.Values["delivery_id"].(string)
		if deliveryID == "" {
			continue
		}
		// Re-enqueue as a fresh stream entry and ACK the stale PEL entry
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: DeliveryStream,
			Values: map[string]any{"delivery_id": deliveryID},
		})
		pipe.XAck(ctx, DeliveryStream, ConsumerGroup, msg.ID)
		slog.Warn("reclaimed stale delivery", "delivery_id", deliveryID, "stream_id", msg.ID)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return 0, err
	}
	return len(result), nil
}
