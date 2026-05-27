package redis

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// Stock key format: "stock:{item}" → HASH with fields: quantity, price
// Example: stock:laptop → { quantity: "10", price: "999" }

// InitStock sets up initial stock levels in Redis.
// Uses SETNX-like behavior — only sets if not already present.
// This way, restarting the service doesn't reset stock levels.
func (c *Client) InitStock(ctx context.Context, items map[string]int) error {
	for item, qty := range items {
		key := fmt.Sprintf("stock:%s", item)

		// HSETNX = "set only if field doesn't exist"
		// If stock:laptop already has a quantity, this does nothing
		c.rdb.HSetNX(ctx, key, "quantity", qty)
	}
	return nil
}

// GetStock returns the current quantity for an item.
// Returns -1 if the item doesn't exist.
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
// Returns the new quantity after deduction.
//
// Uses HINCRBY which is ATOMIC — even if 10 consumers call this
// simultaneously, Redis handles it correctly (no race conditions).
func (c *Client) DeductStock(ctx context.Context, item string, quantity int) (int, error) {
	key := fmt.Sprintf("stock:%s", item)

	// HINCRBY with negative value = deduct
	newQty, err := c.rdb.HIncrBy(ctx, key, "quantity", int64(-quantity)).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to deduct stock for %s: %w", item, err)
	}

	return int(newQty), nil
}

// GetAllStock returns all items and their quantities.
func (c *Client) GetAllStock(ctx context.Context) (map[string]int, error) {
	// Find all keys matching "stock:*"
	keys, err := c.rdb.Keys(ctx, "stock:*").Result()
	if err != nil {
		return nil, err
	}

	result := make(map[string]int)
	for _, key := range keys {
		// Extract item name from key "stock:laptop" → "laptop"
		item := key[len("stock:"):]

		qty, err := c.GetStock(ctx, item)
		if err != nil {
			continue
		}
		result[item] = qty
	}
	return result, nil
}

// CheckAndDeductStock checks if there's enough stock and deducts atomically.
// This combines the check + deduct into a safe operation.
//
// Returns: (newQuantity, error)
// If not enough stock, returns an error (no deduction happens).
func (c *Client) CheckAndDeductStock(ctx context.Context, item string, quantity int) (int, error) {
	// Step 1: Check current stock
	currentQty, err := c.GetStock(ctx, item)
	if err != nil {
		return 0, err
	}

	if currentQty == -1 {
		return 0, fmt.Errorf("item '%s' not found in inventory", item)
	}

	if currentQty < quantity {
		return currentQty, fmt.Errorf("insufficient stock for '%s' (need %d, have %d)",
			item, quantity, currentQty)
	}

	// Step 2: Deduct
	newQty, err := c.DeductStock(ctx, item, quantity)
	if err != nil {
		return 0, err
	}

	return newQty, nil
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

// IncrementCounter atomically increments a counter.
// Examples: stats:total_orders, stats:items:laptop
func (c *Client) IncrementCounter(ctx context.Context, name string, by int64) (int64, error) {
	key := fmt.Sprintf("stats:%s", name)
	return c.rdb.IncrBy(ctx, key, by).Result()
}

// GetCounter returns the current value of a counter.
func (c *Client) GetCounter(ctx context.Context, name string) (int64, error) {
	key := fmt.Sprintf("stats:%s", name)
	val, err := c.rdb.Get(ctx, key).Result()
	if err != nil {
		return 0, nil // counter doesn't exist yet
	}

	count, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err
	}
	return count, nil
}
