package queue

import (
	"context"

	"github.com/redis/go-redis/v9"
)

const DeliveryStream = "stream:deliveries"

type Producer struct {
	rdb *redis.Client
}

func NewProducer(rdb *redis.Client) *Producer {
	return &Producer{rdb: rdb}
}

// Enqueue adds a delivery ID to the Redis Stream for the worker to pick up.
func (p *Producer) Enqueue(ctx context.Context, deliveryID string) error {
	return p.rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: DeliveryStream,
		Values: map[string]any{"delivery_id": deliveryID},
	}).Err()
}
