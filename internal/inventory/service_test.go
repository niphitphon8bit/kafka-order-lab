package inventory_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/niphitphon8bit/kafka-order-lab/internal/inventory"
	"github.com/niphitphon8bit/kafka-order-lab/internal/models"
)

// ---- Stub ----

// spyStockRepo implements inventory.StockRepository.
// It records IncrementCounter calls so tests can assert which counters were touched.
type spyStockRepo struct {
	// GetStock
	stockQty    int // -1 means not found
	getStockErr error

	// CheckAndDeductStock
	deductResult int
	deductErr    error
	deductCalled bool

	// IsProcessed
	processed    bool
	processedErr error

	// MarkProcessed
	markProcessedErr error

	// IncrementCounter — records every (name, by) call
	incrementCalls []incrementCall

	// GetAllStock / InitStock (unused by service tests, but required by interface)
	allStock    map[string]int
	allStockErr error
}

type incrementCall struct {
	name string
	by   int64
}

func (r *spyStockRepo) InitStock(_ context.Context, _ map[string]int) error { return nil }

func (r *spyStockRepo) GetStock(_ context.Context, _ string) (int, error) {
	return r.stockQty, r.getStockErr
}

func (r *spyStockRepo) GetAllStock(_ context.Context) (map[string]int, error) {
	return r.allStock, r.allStockErr
}

func (r *spyStockRepo) CheckAndDeductStock(_ context.Context, _ string, _ int) (int, error) {
	r.deductCalled = true
	return r.deductResult, r.deductErr
}

func (r *spyStockRepo) GetAllItemStats(_ context.Context) (map[string]int64, error) {
	return nil, nil
}

func (r *spyStockRepo) GetCounter(_ context.Context, _ string) (int64, error) { return 0, nil }

func (r *spyStockRepo) IncrementCounter(_ context.Context, name string, by int64) (int64, error) {
	r.incrementCalls = append(r.incrementCalls, incrementCall{name, by})
	return 0, nil
}

func (r *spyStockRepo) IsProcessed(_ context.Context, _ string) (bool, error) {
	return r.processed, r.processedErr
}

func (r *spyStockRepo) MarkProcessed(_ context.Context, _ string, _ time.Duration) error {
	return r.markProcessedErr
}

// hasIncrement returns true if a counter with the given name was incremented.
func (r *spyStockRepo) hasIncrement(name string) bool {
	for _, c := range r.incrementCalls {
		if c.name == name {
			return true
		}
	}
	return false
}

// ---- ProcessOrder tests ----

func TestProcessOrder_HappyPath(t *testing.T) {
	repo := &spyStockRepo{
		processed:    false,
		deductResult: 7,
	}
	svc := inventory.NewStockService(repo)

	order := models.Order{ID: "order-1", Item: "laptop", Quantity: 3}
	err := svc.ProcessOrder(context.Background(), order)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !repo.deductCalled {
		t.Error("expected CheckAndDeductStock to be called")
	}
	if !repo.hasIncrement("total_orders") {
		t.Error("expected total_orders counter to be incremented")
	}
	if !repo.hasIncrement("items:laptop") {
		t.Error("expected items:laptop counter to be incremented")
	}
	if repo.hasIncrement("failed_orders") {
		t.Error("expected failed_orders NOT to be incremented on success")
	}
}

// Duplicate Kafka delivery — the order was already processed.
// Must return nil (commit the offset) and skip all side effects.
func TestProcessOrder_AlreadyProcessed_Idempotent(t *testing.T) {
	repo := &spyStockRepo{processed: true}
	svc := inventory.NewStockService(repo)

	err := svc.ProcessOrder(context.Background(), models.Order{ID: "dup-1", Item: "laptop", Quantity: 1})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if repo.deductCalled {
		t.Error("expected CheckAndDeductStock NOT to be called for duplicate")
	}
	if len(repo.incrementCalls) > 0 {
		t.Error("expected no counter increments for duplicate")
	}
}

// Insufficient stock at deduction time — e.g. race between gRPC check and Kafka consume.
// Must return nil (commit offset, don't retry) and increment failed_orders.
func TestProcessOrder_InsufficientStock_CountsFailure(t *testing.T) {
	repo := &spyStockRepo{
		processed: false,
		deductErr: errors.New("insufficient stock: need 5, have 2"),
	}
	svc := inventory.NewStockService(repo)

	err := svc.ProcessOrder(context.Background(), models.Order{ID: "order-2", Item: "laptop", Quantity: 5})

	if err != nil {
		t.Fatalf("expected nil (not a retryable error), got: %v", err)
	}
	if !repo.hasIncrement("failed_orders") {
		t.Error("expected failed_orders to be incremented")
	}
	if repo.hasIncrement("total_orders") {
		t.Error("expected total_orders NOT to be incremented on failure")
	}
}

// Idempotency check itself fails (Redis down).
// Must return the error so the Kafka consumer retries later.
func TestProcessOrder_IdempotencyCheckFails_ReturnsError(t *testing.T) {
	repo := &spyStockRepo{
		processedErr: errors.New("redis: connection refused"),
	}
	svc := inventory.NewStockService(repo)

	err := svc.ProcessOrder(context.Background(), models.Order{ID: "order-3", Item: "laptop", Quantity: 1})

	if err == nil {
		t.Fatal("expected error when idempotency check fails, got nil")
	}
	if repo.deductCalled {
		t.Error("expected CheckAndDeductStock NOT to be called when idempotency check fails")
	}
}

// ---- CheckStock tests ----

func TestCheckStock_SufficientStock(t *testing.T) {
	repo := &spyStockRepo{stockQty: 10}
	svc := inventory.NewStockService(repo)

	available, current, msg, err := svc.CheckStock(context.Background(), "laptop", 5)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !available {
		t.Errorf("available: got false, want true")
	}
	if current != 10 {
		t.Errorf("current: got %d, want %d", current, 10)
	}
	if msg == "" {
		t.Error("expected non-empty message")
	}
}

func TestCheckStock_InsufficientStock(t *testing.T) {
	repo := &spyStockRepo{stockQty: 2}
	svc := inventory.NewStockService(repo)

	available, current, _, err := svc.CheckStock(context.Background(), "laptop", 5)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if available {
		t.Error("available: got true, want false")
	}
	if current != 2 {
		t.Errorf("current: got %d, want %d", current, 2)
	}
}

// GetStock returns -1 to signal "item not found".
func TestCheckStock_ItemNotFound(t *testing.T) {
	repo := &spyStockRepo{stockQty: -1}
	svc := inventory.NewStockService(repo)

	available, current, msg, err := svc.CheckStock(context.Background(), "unknown-item", 1)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if available {
		t.Error("available: got true, want false for unknown item")
	}
	if current != 0 {
		t.Errorf("current: got %d, want 0 for unknown item", current)
	}
	if msg == "" {
		t.Error("expected a 'not found' message")
	}
}

func TestCheckStock_RepoError(t *testing.T) {
	repo := &spyStockRepo{getStockErr: errors.New("redis down")}
	svc := inventory.NewStockService(repo)

	_, _, _, err := svc.CheckStock(context.Background(), "laptop", 1)

	if err == nil {
		t.Fatal("expected error when repo fails, got nil")
	}
}
