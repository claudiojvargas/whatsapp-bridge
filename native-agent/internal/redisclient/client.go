package redisclient

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type Client struct {
	client *redis.Client

	eventsStream  string
	commandsQueue string
}

func New(
	addr string,
	password string,
	db int,
	eventsStream string,
	commandsQueue string,
) *Client {
	rdb := redis.NewClient(
		&redis.Options{
			Addr:     addr,
			Password: password,
			DB:       db,
		},
	)

	return &Client{
		client:        rdb,
		eventsStream:  eventsStream,
		commandsQueue: commandsQueue,
	}
}

func (c *Client) Ping(
	ctx context.Context,
) error {
	if err := c.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf(
			"redis ping failed: %w",
			err,
		)
	}

	return nil
}

func (c *Client) PublishEvent(
	ctx context.Context,
	eventType string,
	fields map[string]any,
) (string, error) {
	values := make(
		map[string]any,
		len(fields)+2,
	)

	values["type"] = eventType

	values["created_at"] = time.Now().
		UTC().
		Format(time.RFC3339Nano)

	for key, value := range fields {
		values[key] = value
	}

	id, err := c.client.XAdd(
		ctx,
		&redis.XAddArgs{
			Stream: c.eventsStream,
			Values: values,
		},
	).Result()

	if err != nil {
		return "", fmt.Errorf(
			"publish redis event: %w",
			err,
		)
	}

	return id, nil
}

func (c *Client) WaitCommand(
	ctx context.Context,
	timeout time.Duration,
) (string, error) {
	result, err := c.client.BLPop(
		ctx,
		timeout,
		c.commandsQueue,
	).Result()

	if errors.Is(err, redis.Nil) {
		return "", nil
	}

	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}

		return "", fmt.Errorf(
			"wait redis command: %w",
			err,
		)
	}

	if len(result) != 2 {
		return "", fmt.Errorf(
			"unexpected BLPOP response length: %d",
			len(result),
		)
	}

	return result[1], nil
}

func (c *Client) Close() error {
	return c.client.Close()
}
