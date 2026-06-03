package inventory_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/niphitphon8bit/kafka-order-lab/internal/infrastructure/pb"
	"github.com/niphitphon8bit/kafka-order-lab/internal/inventory"
	"github.com/niphitphon8bit/kafka-order-lab/internal/models"
)

// ---- Stub ----

type stubStockService struct {
	checkAvail   bool
	checkCurrent int32
	checkMsg     string
	checkErr     error

	processErr    error
	processCalled bool
	processedOrder models.Order

	allStock    map[string]int
	allStockErr error
}

func (s *stubStockService) InitStock(_ context.Context, _ map[string]int) error { return nil }

func (s *stubStockService) CheckStock(_ context.Context, _ string, _ int) (bool, int32, string, error) {
	return s.checkAvail, s.checkCurrent, s.checkMsg, s.checkErr
}

func (s *stubStockService) GetAllStock(_ context.Context) (map[string]int, error) {
	return s.allStock, s.allStockErr
}

func (s *stubStockService) ProcessOrder(_ context.Context, order models.Order) error {
	s.processCalled = true
	s.processedOrder = order
	return s.processErr
}

// ---- KafkaHandler tests ----

func TestKafkaHandler_ValidOrderCreated_CallsProcessOrder(t *testing.T) {
	svc := &stubStockService{}
	h := inventory.NewKafkaHandler(svc)

	order := models.Order{ID: "order-1", Item: "laptop", Quantity: 2}
	event := models.OrderEvent{EventType: "order.created", Order: order}
	payload, _ := json.Marshal(event)

	err := h.Handle("order-1", payload)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !svc.processCalled {
		t.Error("expected ProcessOrder to be called")
	}
	if svc.processedOrder.ID != "order-1" {
		t.Errorf("order ID: got %q, want %q", svc.processedOrder.ID, "order-1")
	}
	if svc.processedOrder.Item != "laptop" {
		t.Errorf("order item: got %q, want %q", svc.processedOrder.Item, "laptop")
	}
}

func TestKafkaHandler_InvalidJSON_ReturnsError(t *testing.T) {
	h := inventory.NewKafkaHandler(&stubStockService{})

	err := h.Handle("key", []byte("not-valid-json{{{"))

	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

// Unknown event types must be silently skipped (return nil) so the offset is
// committed and the consumer doesn't get stuck on unrecognised messages.
func TestKafkaHandler_UnknownEventType_SkipsGracefully(t *testing.T) {
	svc := &stubStockService{}
	h := inventory.NewKafkaHandler(svc)

	event := models.OrderEvent{EventType: "order.shipped"}
	payload, _ := json.Marshal(event)

	err := h.Handle("key", payload)

	if err != nil {
		t.Fatalf("expected nil for unknown event type, got: %v", err)
	}
	if svc.processCalled {
		t.Error("expected ProcessOrder NOT to be called for unknown event type")
	}
}

func TestKafkaHandler_ProcessOrderError_Propagates(t *testing.T) {
	svc := &stubStockService{processErr: errors.New("idempotency check failed")}
	h := inventory.NewKafkaHandler(svc)

	event := models.OrderEvent{EventType: "order.created", Order: models.Order{ID: "order-x"}}
	payload, _ := json.Marshal(event)

	err := h.Handle("order-x", payload)

	if err == nil {
		t.Fatal("expected error to propagate from ProcessOrder, got nil")
	}
}

// ---- GRPCHandler tests ----

func TestGRPCHandler_CheckStock_Available(t *testing.T) {
	svc := &stubStockService{checkAvail: true, checkCurrent: 8, checkMsg: "8 in stock"}
	h := inventory.NewGRPCHandler(svc)

	resp, err := h.CheckStock(context.Background(), &pb.CheckStockRequest{Item: "laptop", Quantity: 2})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Available {
		t.Error("available: got false, want true")
	}
	if resp.CurrentStock != 8 {
		t.Errorf("CurrentStock: got %d, want %d", resp.CurrentStock, 8)
	}
	if resp.Message != "8 in stock" {
		t.Errorf("Message: got %q, want %q", resp.Message, "8 in stock")
	}
}

func TestGRPCHandler_CheckStock_Unavailable(t *testing.T) {
	svc := &stubStockService{checkAvail: false, checkCurrent: 1, checkMsg: "insufficient stock (need 5, have 1)"}
	h := inventory.NewGRPCHandler(svc)

	resp, err := h.CheckStock(context.Background(), &pb.CheckStockRequest{Item: "laptop", Quantity: 5})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Available {
		t.Error("available: got true, want false")
	}
	if resp.CurrentStock != 1 {
		t.Errorf("CurrentStock: got %d, want %d", resp.CurrentStock, 1)
	}
}

func TestGRPCHandler_CheckStock_ServiceError(t *testing.T) {
	svc := &stubStockService{checkErr: errors.New("redis down")}
	h := inventory.NewGRPCHandler(svc)

	_, err := h.CheckStock(context.Background(), &pb.CheckStockRequest{Item: "laptop", Quantity: 1})

	if err == nil {
		t.Fatal("expected error when service fails, got nil")
	}
}

func TestGRPCHandler_GetStock_MapsAllItems(t *testing.T) {
	svc := &stubStockService{allStock: map[string]int{"laptop": 8, "mouse": 45}}
	h := inventory.NewGRPCHandler(svc)

	resp, err := h.GetStock(context.Background(), &pb.GetStockRequest{})

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Items) != 2 {
		t.Fatalf("items count: got %d, want %d", len(resp.Items), 2)
	}

	// Build a map from the response for order-independent assertion
	got := make(map[string]int32, len(resp.Items))
	for _, item := range resp.Items {
		got[item.Item] = item.Quantity
	}
	if got["laptop"] != 8 {
		t.Errorf("laptop quantity: got %d, want %d", got["laptop"], 8)
	}
	if got["mouse"] != 45 {
		t.Errorf("mouse quantity: got %d, want %d", got["mouse"], 45)
	}
}

func TestGRPCHandler_GetStock_ServiceError(t *testing.T) {
	svc := &stubStockService{allStockErr: errors.New("redis down")}
	h := inventory.NewGRPCHandler(svc)

	_, err := h.GetStock(context.Background(), &pb.GetStockRequest{})

	if err == nil {
		t.Fatal("expected error when service fails, got nil")
	}
}
