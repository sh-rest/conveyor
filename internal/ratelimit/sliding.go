package ratelimit

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Limiter struct {
	rdb *redis.Client
}

func NewLimiter(rdb *redis.Client) *Limiter {
	return &Limiter{rdb: rdb}
}

// Allow reports whether a delivery to endpointID is within its rate limit.
// Uses a 1-second sliding window log stored as a Redis sorted set.
// deliveryID is the sorted set member — guarantees uniqueness within the window.
// Fails open: if Redis is unavailable, returns (false, err) and the caller should allow.
func (l *Limiter) Allow(ctx context.Context, endpointID, deliveryID string, limitRPS int32) (bool, error) {
	key := fmt.Sprintf("rl:%s", endpointID)
	now := time.Now().UnixMilli()
	windowStart := now - 1000 // 1-second sliding window

	// Step 1: evict expired entries and count what remains in the window.
	pipe := l.rdb.Pipeline()
	pipe.ZRemRangeByScore(ctx, key, "0", strconv.FormatInt(windowStart, 10))
	countCmd := pipe.ZCard(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}

	if countCmd.Val() >= int64(limitRPS) {
		return false, nil // throttled
	}

	// Step 2: record this request and set TTL so the key auto-cleans if the endpoint goes idle.
	pipe2 := l.rdb.Pipeline()
	pipe2.ZAdd(ctx, key, redis.Z{Score: float64(now), Member: deliveryID})
	pipe2.Expire(ctx, key, 5*time.Second)
	_, err := pipe2.Exec(ctx)
	return err == nil, err
}
