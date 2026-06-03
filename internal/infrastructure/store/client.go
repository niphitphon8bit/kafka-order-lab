package store

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client wraps the go-redis client with our helper methods
type Client struct {
	rdb *redis.Client
}

// NewClient connects to Redis
func NewClient(addr string) (*Client, error) {
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,          // e.g. "localhost:6379"
		Password: "",            // no password for local dev
		DB:       0,             // use default DB
	})

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis at %s: %w", addr, err)
	}

	log.Printf("Redis connected to %s", addr)
	return &Client{rdb: rdb}, nil
}

// Close shuts down the Redis connection
func (c *Client) Close() error {
	return c.rdb.Close()
}
