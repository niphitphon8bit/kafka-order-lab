//go:build integration

package store

// Integration tests for the Redis store layer.
//
// These tests require a real Redis instance and are excluded from the normal
// test suite. Run them with:
//
//	go test -tags integration ./internal/infrastructure/store/...
//
// By default they connect to localhost:6379 DB 15.
// Override with REDIS_TEST_ADDR env var (e.g. "localhost:6380").
// DB 15 is flushed before and after every test — use a dedicated Redis
// instance or ensure DB 15 is safe to wipe.

import (
	"context"
	"os"
	"testing"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// newTestClient connects to Redis DB 15 and flushes it.
// The test is skipped if Redis is not reachable.
func newTestClient(t *testing.T) *Client {
	t.Helper()

	addr := os.Getenv("REDIS_TEST_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	rdb := goredis.NewClient(&goredis.Options{Addr: addr, DB: 15})
	ctx := context.Background()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available at %s (DB 15): %v", addr, err)
	}

	rdb.FlushDB(ctx)

	c := &Client{rdb: rdb}
	t.Cleanup(func() {
		rdb.FlushDB(ctx)
		rdb.Close()
	})
	return c
}

// ---- CheckAndDeductStock (Lua script) ----

func TestCheckAndDeductStock_SufficientStock(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	c.rdb.HSet(ctx, "stock:laptop", "quantity", 10)

	newQty, err := c.CheckAndDeductStock(ctx, "laptop", 3)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if newQty != 7 {
		t.Errorf("newQty: got %d, want %d", newQty, 7)
	}

	// Verify Redis actually has the new value
	got, _ := c.GetStock(ctx, "laptop")
	if got != 7 {
		t.Errorf("Redis stock after deduct: got %d, want %d", got, 7)
	}
}

func TestCheckAndDeductStock_InsufficientStock_NoChange(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	c.rdb.HSet(ctx, "stock:laptop", "quantity", 2)

	_, err := c.CheckAndDeductStock(ctx, "laptop", 5)

	if err == nil {
		t.Fatal("expected error for insufficient stock, got nil")
	}

	// Stock must be unchanged — the Lua script must not have deducted anything
	got, _ := c.GetStock(ctx, "laptop")
	if got != 2 {
		t.Errorf("stock must be unchanged after failed deduct: got %d, want %d", got, 2)
	}
}

func TestCheckAndDeductStock_ItemNotFound(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	// No stock key set at all
	_, err := c.CheckAndDeductStock(ctx, "unknown-item", 1)

	if err == nil {
		t.Fatal("expected error for unknown item, got nil")
	}
}

// DeductToZero verifies that the Lua script allows deducting to exactly 0.
func TestCheckAndDeductStock_DeductToZero(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	c.rdb.HSet(ctx, "stock:laptop", "quantity", 3)

	newQty, err := c.CheckAndDeductStock(ctx, "laptop", 3)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if newQty != 0 {
		t.Errorf("newQty: got %d, want 0", newQty)
	}
}

// ---- InitStock (HSETNX behaviour) ----

// InitStock must NOT overwrite existing values — restarting the service
// should preserve whatever stock currently exists in Redis.
func TestInitStock_DoesNotOverwriteExistingValues(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	// Simulate current state: some orders have already been processed
	c.rdb.HSet(ctx, "stock:laptop", "quantity", 3)

	err := c.InitStock(ctx, map[string]int{"laptop": 10, "mouse": 50})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// laptop was already set — must still be 3, not reset to 10
	laptop, _ := c.GetStock(ctx, "laptop")
	if laptop != 3 {
		t.Errorf("laptop stock: got %d, want 3 (must not be overwritten)", laptop)
	}

	// mouse was not set — InitStock should seed it
	mouse, _ := c.GetStock(ctx, "mouse")
	if mouse != 50 {
		t.Errorf("mouse stock: got %d, want 50 (new item should be seeded)", mouse)
	}
}

// ---- IsProcessed / MarkProcessed ----

func TestMarkProcessed_IsProcessed_RoundTrip(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	// Not processed yet
	processed, err := c.IsProcessed(ctx, "order-abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processed {
		t.Error("expected order to not be processed initially")
	}

	// Mark it
	if err := c.MarkProcessed(ctx, "order-abc", time.Minute); err != nil {
		t.Fatalf("MarkProcessed failed: %v", err)
	}

	// Now it should be processed
	processed, err = c.IsProcessed(ctx, "order-abc")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !processed {
		t.Error("expected order to be processed after MarkProcessed")
	}
}

func TestMarkProcessed_DifferentOrdersAreIndependent(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	c.MarkProcessed(ctx, "order-1", time.Minute)

	p1, _ := c.IsProcessed(ctx, "order-1")
	p2, _ := c.IsProcessed(ctx, "order-2")

	if !p1 {
		t.Error("order-1 should be processed")
	}
	if p2 {
		t.Error("order-2 should NOT be processed")
	}
}

// ---- GetAllStock (SCAN) ----

func TestGetAllStock_ReturnsAllSeededItems(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	seed := map[string]int{"laptop": 10, "mouse": 50, "keyboard": 30}
	for item, qty := range seed {
		c.rdb.HSet(ctx, "stock:"+item, "quantity", qty)
	}

	got, err := c.GetAllStock(ctx)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != len(seed) {
		t.Errorf("item count: got %d, want %d", len(got), len(seed))
	}
	for item, wantQty := range seed {
		if got[item] != wantQty {
			t.Errorf("%s: got %d, want %d", item, got[item], wantQty)
		}
	}
}

func TestGetAllStock_EmptyReturnsEmptyMap(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	got, err := c.GetAllStock(ctx)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected empty map, got %v", got)
	}
}

// ---- IncrementCounter / GetCounter ----

func TestIncrementCounter_GetCounter_RoundTrip(t *testing.T) {
	c := newTestClient(t)
	ctx := context.Background()

	// Counter starts at zero (non-existent key)
	val, _ := c.GetCounter(ctx, "total_orders")
	if val != 0 {
		t.Errorf("initial value: got %d, want 0", val)
	}

	c.IncrementCounter(ctx, "total_orders", 1)
	c.IncrementCounter(ctx, "total_orders", 1)
	c.IncrementCounter(ctx, "total_orders", 3)

	val, err := c.GetCounter(ctx, "total_orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if val != 5 {
		t.Errorf("after increments: got %d, want 5", val)
	}
}
