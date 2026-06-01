package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// Stock key format: "stock:{item}" → HASH with field: quantity
// Example: HGET stock:laptop quantity → "10"

// checkAndDeductScript atomically checks stock and deducts if sufficient.
//
// Why Lua? Redis executes Lua scripts as a single atomic operation — no other
// command can run between the check and the deduction. This eliminates the
// TOCTOU race that exists when doing two separate round-trips (HGET + HINCRBY).
//
// KEYS[1] = stock key (e.g. "stock:laptop")
// ARGV[1] = quantity to deduct (as string)
// Returns: new quantity as integer, or Redis error string on failure
var checkAndDeductScript = redis.NewScript(`
local current = tonumber(redis.call('HGET', KEYS[1], 'quantity'))
if current == nil then
  return redis.error_reply('item not found')
end
local qty = tonumber(ARGV[1])
if current < qty then
  return redis.error_reply('insufficient stock: need ' .. qty .. ', have ' .. current)
end
return redis.call('HINCRBY', KEYS[1], 'quantity', -qty)
`)

// InitStock sets initial stock levels using HSETNX (set-only-if-not-exists).
// Restarting the service does NOT reset stock because existing keys are skipped.
func (c *Client) InitStock(ctx context.Context, items map[string]int) error {
	for item, qty := range items {
		key := fmt.Sprintf("stock:%s", item)
		c.rdb.HSetNX(ctx, key, "quantity", qty)
	}
	return nil
}

// GetStock returns the current quantity for an item.
// Returns -1 if the item does not exist.
func (c *Client) GetStock(ctx context.Context, item string) (int, error) {
	key := fmt.Sprintf("stock:%s", item)
	val, err := c.rdb.HGet(ctx, key, "quantity").Result()
	if err != nil {
		return -1, nil // item not found
	}

	qty, err := strconv.Atoi(val)
	if err != nil {
		return -1, fmt.Errorf("invalid stock value for %s: %w", item, err)
	}
	return qty, nil
}

// DeductStock atomically decreases stock for an item.
// Uses HINCRBY which is a single atomic Redis command.
func (c *Client) DeductStock(ctx context.Context, item string, quantity int) (int, error) {
	key := fmt.Sprintf("stock:%s", item)
	newQty, err := c.rdb.HIncrBy(ctx, key, "quantity", int64(-quantity)).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to deduct stock for %s: %w", item, err)
	}
	return int(newQty), nil
}

// GetAllStock returns all items and their quantities.
// Uses cursor-based SCAN instead of KEYS to avoid blocking Redis.
func (c *Client) GetAllStock(ctx context.Context) (map[string]int, error) {
	result := make(map[string]int)
	var cursor uint64

	for {
		keys, next, err := c.rdb.Scan(ctx, cursor, "stock:*", 100).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			item := strings.TrimPrefix(key, "stock:")
			qty, err := c.GetStock(ctx, item)
			if err != nil {
				continue
			}
			result[item] = qty
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}

	return result, nil
}

// CheckAndDeductStock atomically checks if enough stock exists and deducts it.
// Uses a Lua script so the check and deduct happen as one Redis operation,
// eliminating the race window between reading and writing.
//
// Returns: (newQuantity, error). On insufficient stock or missing item, returns an error.
func (c *Client) CheckAndDeductStock(ctx context.Context, item string, quantity int) (int, error) {
	key := fmt.Sprintf("stock:%s", item)

	result, err := checkAndDeductScript.Run(ctx, c.rdb, []string{key}, quantity).Int()
	if err != nil {
		return 0, fmt.Errorf("stock deduction failed for '%s': %w", item, err)
	}

	return result, nil
}

// ---- Idempotency ----

// IsProcessed checks if an order has already been processed.
func (c *Client) IsProcessed(ctx context.Context, orderID string) (bool, error) {
	key := fmt.Sprintf("processed:%s", orderID)
	result, err := c.rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check processed status: %w", err)
	}
	return result == 1, nil
}

// MarkProcessed marks an order as processed with a TTL.
// After the TTL expires, the key is automatically deleted.
func (c *Client) MarkProcessed(ctx context.Context, orderID string, ttl time.Duration) error {
	key := fmt.Sprintf("processed:%s", orderID)
	return c.rdb.Set(ctx, key, "done", ttl).Err()
}

// ---- Analytics Counters ----

// IncrementCounter atomically increments a named counter.
// Counter names map to Redis keys: "total_orders" → "stats:total_orders"
func (c *Client) IncrementCounter(ctx context.Context, name string, by int64) (int64, error) {
	key := fmt.Sprintf("stats:%s", name)
	return c.rdb.IncrBy(ctx, key, by).Result()
}

// GetCounter returns the current value of a named counter.
func (c *Client) GetCounter(ctx context.Context, name string) (int64, error) {
	key := fmt.Sprintf("stats:%s", name)
	val, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		return 0, nil // counter does not exist yet
	}

	count, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err
	}
	return count, nil
}

// GetAllItemStats returns per-item quantities sold by scanning "stats:items:*" keys.
// Discovers items dynamically — no hardcoded item list required.
func (c *Client) GetAllItemStats(ctx context.Context) (map[string]int64, error) {
	result := make(map[string]int64)
	var cursor uint64

	for {
		keys, next, err := c.rdb.Scan(ctx, cursor, "stats:items:*", 100).Result()
		if err != nil {
			return nil, err
		}

		for _, key := range keys {
			item := strings.TrimPrefix(key, "stats:items:")
			val, err := c.GetCounter(ctx, "items:"+item)
			if err == nil {
				result[item] = val
			}
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}

	return result, nil
}
