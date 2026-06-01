package order_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/niphitphon8bit/kafka-order-lab/internal/models"
	"github.com/niphitphon8bit/kafka-order-lab/internal/order"
)

// stubOrderService is a minimal OrderService implementation for handler tests.
// It lets each test control what the service returns without touching Redis or Kafka.
type stubOrderService struct {
	createOrderFn func(ctx context.Context, req models.CreateOrderRequest) (models.Order, error)
}

func (s *stubOrderService) CreateOrder(ctx context.Context, req models.CreateOrderRequest) (models.Order, error) {
	if s.createOrderFn != nil {
		return s.createOrderFn(ctx, req)
	}
	return models.Order{ID: "test-id", Item: req.Item, Quantity: req.Quantity, Status: "created"}, nil
}

func (s *stubOrderService) GetStocks(ctx context.Context) (map[string]int, error) {
	return map[string]int{"laptop": 10}, nil
}

func (s *stubOrderService) GetStats(ctx context.Context) (models.Stats, error) {
	return models.Stats{TotalOrders: 5, FailedOrders: 1, ItemsSold: map[string]int64{"laptop": 4}}, nil
}

// ---- Tests ----

func TestHealthHandler(t *testing.T) {
	h := order.NewOrderHandler(&stubOrderService{})

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", w.Code, http.StatusOK)
	}

	var body map[string]string
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if body["status"] != "ok" {
		t.Errorf("status: got %q, want %q", body["status"], "ok")
	}
	if body["service"] != "order-service" {
		t.Errorf("service: got %q, want %q", body["service"], "order-service")
	}
}

func TestCreateOrder_InvalidBody(t *testing.T) {
	h := order.NewOrderHandler(&stubOrderService{})

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/orders", strings.NewReader("not-json"))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestCreateOrder_MissingFields(t *testing.T) {
	h := order.NewOrderHandler(&stubOrderService{})

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	cases := []struct {
		name string
		body string
	}{
		{"empty item", `{"item":"","quantity":1}`},
		{"zero quantity", `{"item":"laptop","quantity":0}`},
		{"negative quantity", `{"item":"laptop","quantity":-1}`},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/orders", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Errorf("status: got %d, want %d", w.Code, http.StatusBadRequest)
			}
		})
	}
}

func TestCreateOrder_InsufficientStock_Returns409(t *testing.T) {
	svc := &stubOrderService{
		createOrderFn: func(_ context.Context, req models.CreateOrderRequest) (models.Order, error) {
			return models.Order{}, &order.InsufficientStockError{
				Message:      "insufficient stock (need 5, have 2)",
				CurrentStock: 2,
			}
		},
	}
	h := order.NewOrderHandler(svc)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/orders", strings.NewReader(`{"item":"laptop","quantity":5}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusConflict {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusConflict)
	}

	var body map[string]any
	json.NewDecoder(w.Body).Decode(&body)
	if body["current_stock"] == nil {
		t.Error("expected current_stock in response body")
	}
}

func TestCreateOrder_Success_Returns201(t *testing.T) {
	h := order.NewOrderHandler(&stubOrderService{})

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/orders", strings.NewReader(`{"item":"laptop","quantity":1}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Errorf("status: got %d, want %d", w.Code, http.StatusCreated)
	}

	var body models.Order
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if body.Item != "laptop" {
		t.Errorf("item: got %q, want %q", body.Item, "laptop")
	}
}
