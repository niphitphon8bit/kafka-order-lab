package order_test

import (
	"context"
	"errors"
	"testing"

	"github.com/niphitphon8bit/kafka-order-lab/internal/models"
	"github.com/niphitphon8bit/kafka-order-lab/internal/order"
)

// ---- Stubs ----

type stubStockReader struct {
	stocks       map[string]int
	stocksErr    error
	itemStats    map[string]int64
	itemStatsErr error
	counters     map[string]int64
}

func (s *stubStockReader) GetAllStock(_ context.Context) (map[string]int, error) {
	return s.stocks, s.stocksErr
}

func (s *stubStockReader) GetAllItemStats(_ context.Context) (map[string]int64, error) {
	return s.itemStats, s.itemStatsErr
}

func (s *stubStockReader) GetCounter(_ context.Context, name string) (int64, error) {
	return s.counters[name], nil
}

type stubInventoryChecker struct {
	available    bool
	currentStock int
	message      string
	err          error
}

func (s *stubInventoryChecker) CheckStock(_ context.Context, _ string, _ int) (bool, int, string, error) {
	return s.available, s.currentStock, s.message, s.err
}

type stubEventPublisher struct {
	err    error
	called bool
}

func (s *stubEventPublisher) SendMessage(_ string, _ any) (int32, int64, error) {
	s.called = true
	return 0, 0, s.err
}

// ---- CreateOrder tests ----

func TestCreateOrder_HappyPath(t *testing.T) {
	publisher := &stubEventPublisher{}
	svc := order.NewOrderService(
		&stubStockReader{},
		&stubInventoryChecker{available: true, currentStock: 10},
		publisher,
	)

	got, err := svc.CreateOrder(context.Background(), models.CreateOrderRequest{Item: "laptop", Quantity: 2})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ID == "" {
		t.Error("expected non-empty order ID")
	}
	if got.Item != "laptop" {
		t.Errorf("item: got %q, want %q", got.Item, "laptop")
	}
	if got.Quantity != 2 {
		t.Errorf("quantity: got %d, want %d", got.Quantity, 2)
	}
	if got.Status != "created" {
		t.Errorf("status: got %q, want %q", got.Status, "created")
	}
	if !publisher.called {
		t.Error("expected Kafka publish to be called")
	}
}

func TestCreateOrder_InsufficientStock(t *testing.T) {
	publisher := &stubEventPublisher{}
	svc := order.NewOrderService(
		&stubStockReader{},
		&stubInventoryChecker{available: false, currentStock: 1, message: "insufficient stock (need 5, have 1)"},
		publisher,
	)

	_, err := svc.CreateOrder(context.Background(), models.CreateOrderRequest{Item: "laptop", Quantity: 5})

	if err == nil {
		t.Fatal("expected error, got nil")
	}
	var stockErr *order.InsufficientStockError
	if !errors.As(err, &stockErr) {
		t.Fatalf("expected InsufficientStockError, got %T: %v", err, err)
	}
	if stockErr.CurrentStock != 1 {
		t.Errorf("CurrentStock: got %d, want %d", stockErr.CurrentStock, 1)
	}
	if publisher.called {
		t.Error("expected NO Kafka publish when stock is insufficient")
	}
}

// When gRPC is unreachable the order should still be published to Kafka.
// The Kafka consumer is the safety net — it will reject at deduction time if needed.
func TestCreateOrder_GRPCFailure_ProceedsAnyway(t *testing.T) {
	publisher := &stubEventPublisher{}
	svc := order.NewOrderService(
		&stubStockReader{},
		&stubInventoryChecker{err: errors.New("connection refused")},
		publisher,
	)

	got, err := svc.CreateOrder(context.Background(), models.CreateOrderRequest{Item: "laptop", Quantity: 1})

	if err != nil {
		t.Fatalf("expected no error on gRPC failure (fallback), got: %v", err)
	}
	if got.ID == "" {
		t.Error("expected valid order to be returned")
	}
	if !publisher.called {
		t.Error("expected Kafka publish despite gRPC failure")
	}
}

func TestCreateOrder_PublishFailure(t *testing.T) {
	svc := order.NewOrderService(
		&stubStockReader{},
		&stubInventoryChecker{available: true, currentStock: 10},
		&stubEventPublisher{err: errors.New("kafka unavailable")},
	)

	_, err := svc.CreateOrder(context.Background(), models.CreateOrderRequest{Item: "laptop", Quantity: 1})

	if err == nil {
		t.Fatal("expected error when Kafka publish fails, got nil")
	}
}

// ---- GetStocks tests ----

func TestGetStocks_HappyPath(t *testing.T) {
	want := map[string]int{"laptop": 8, "mouse": 45}
	svc := order.NewOrderService(
		&stubStockReader{stocks: want},
		&stubInventoryChecker{},
		&stubEventPublisher{},
	)

	got, err := svc.GetStocks(context.Background())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got["laptop"] != 8 {
		t.Errorf("laptop: got %d, want %d", got["laptop"], 8)
	}
	if got["mouse"] != 45 {
		t.Errorf("mouse: got %d, want %d", got["mouse"], 45)
	}
}

func TestGetStocks_RepoError(t *testing.T) {
	svc := order.NewOrderService(
		&stubStockReader{stocksErr: errors.New("redis down")},
		&stubInventoryChecker{},
		&stubEventPublisher{},
	)

	_, err := svc.GetStocks(context.Background())

	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ---- GetStats tests ----

func TestGetStats_HappyPath(t *testing.T) {
	svc := order.NewOrderService(
		&stubStockReader{
			counters:  map[string]int64{"total_orders": 10, "failed_orders": 2},
			itemStats: map[string]int64{"laptop": 8, "mouse": 2},
		},
		&stubInventoryChecker{},
		&stubEventPublisher{},
	)

	stats, err := svc.GetStats(context.Background())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stats.TotalOrders != 10 {
		t.Errorf("TotalOrders: got %d, want %d", stats.TotalOrders, 10)
	}
	if stats.FailedOrders != 2 {
		t.Errorf("FailedOrders: got %d, want %d", stats.FailedOrders, 2)
	}
	if stats.ItemsSold["laptop"] != 8 {
		t.Errorf("ItemsSold[laptop]: got %d, want %d", stats.ItemsSold["laptop"], 8)
	}
}

func TestGetStats_ItemStatsFails(t *testing.T) {
	svc := order.NewOrderService(
		&stubStockReader{itemStatsErr: errors.New("redis down")},
		&stubInventoryChecker{},
		&stubEventPublisher{},
	)

	_, err := svc.GetStats(context.Background())

	if err == nil {
		t.Fatal("expected error when GetAllItemStats fails, got nil")
	}
}
