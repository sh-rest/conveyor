package queue

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	ConsumerGroup = "workers"
	blockDuration = 5 * time.Second
)

// Message is passed through the work channel from the consumer to worker goroutines.
type Message struct {
	StreamID   string // Redis stream entry ID, used for XACK
	DeliveryID string // UUID string of the delivery row
}

type Consumer struct {
	rdb  *redis.Client
	name string
}

func NewConsumer(rdb *redis.Client, name string) *Consumer {
	return &Consumer{rdb: rdb, name: name}
}

// SetupGroup creates the consumer group and stream if they don't exist.
// Safe to call on every startup — ignores "already exists" errors.
func (c *Consumer) SetupGroup(ctx context.Context) error {
	err := c.rdb.XGroupCreateMkStream(ctx, DeliveryStream, ConsumerGroup, "0").Err()
	if err != nil && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		return err
	}
	return nil
}

// Read blocks up to blockDuration waiting for messages. Returns nil (not error) on timeout.
func (c *Consumer) Read(ctx context.Context) ([]Message, error) {
	result, err := c.rdb.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    ConsumerGroup,
		Consumer: c.name,
		Streams:  []string{DeliveryStream, ">"},
		Count:    10,
		Block:    blockDuration,
	}).Result()

	if err == redis.Nil {
		return nil, nil // timeout — no messages, not an error
	}
	if err != nil {
		return nil, err
	}

	var msgs []Message
	for _, stream := range result {
		for _, msg := range stream.Messages {
			dID, _ := msg.Values["delivery_id"].(string)
			msgs = append(msgs, Message{StreamID: msg.ID, DeliveryID: dID})
		}
	}
	return msgs, nil
}

// Ack acknowledges messages, removing them from the PEL.
// Called after a worker has committed the delivery outcome to Postgres.
func (c *Consumer) Ack(ctx context.Context, streamIDs ...string) {
	if err := c.rdb.XAck(ctx, DeliveryStream, ConsumerGroup, streamIDs...).Err(); err != nil {
		slog.Error("failed to ack stream messages", "err", err)
	}
}
