package order

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/niphitphon8bit/kafka-order-lab/internal/models"
)

// OrderHandler holds HTTP handlers for the order service.
// It only knows about HTTP request/response and the OrderService interface —
// no Kafka, Redis, or gRPC details here.
type OrderHandler struct {
	svc OrderService
}

func NewOrderHandler(svc OrderService) *OrderHandler {
	return &OrderHandler{svc: svc}
}

// RegisterRoutes registers all API routes on the provided mux.
// The caller (main.go) is responsible for adding any non-API routes (e.g. static files).
func (h *OrderHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /health", h.health)
	mux.HandleFunc("POST /orders", h.createOrder)
	mux.HandleFunc("GET /api/stocks", h.getStocks)
	mux.HandleFunc("GET /api/stats", h.getStats)
}

func (h *OrderHandler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"service": "order-service",
	})
}

func (h *OrderHandler) createOrder(w http.ResponseWriter, r *http.Request) {
	var req models.CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errBody("invalid request body"))
		return
	}

	if req.Item == "" || req.Quantity <= 0 {
		writeJSON(w, http.StatusBadRequest, errBody("item and quantity are required"))
		return
	}

	order, err := h.svc.CreateOrder(r.Context(), req)
	if err != nil {
		var stockErr *InsufficientStockError
		if errors.As(err, &stockErr) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":         stockErr.Message,
				"current_stock": stockErr.CurrentStock,
			})
			return
		}
		writeJSON(w, http.StatusInternalServerError, errBody("failed to process order"))
		return
	}

	writeJSON(w, http.StatusCreated, order)
}

func (h *OrderHandler) getStocks(w http.ResponseWriter, r *http.Request) {
	stocks, err := h.svc.GetStocks(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("failed to get stocks"))
		return
	}
	writeJSON(w, http.StatusOK, stocks)
}

func (h *OrderHandler) getStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.svc.GetStats(r.Context())
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errBody("failed to get stats"))
		return
	}
	writeJSON(w, http.StatusOK, stats)
}

// writeJSON is a helper to write a JSON response with a given status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func errBody(msg string) map[string]string {
	return map[string]string{"error": msg}
}
