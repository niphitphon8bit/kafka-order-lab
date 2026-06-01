package models

import "time"

// Order represents a customer order
type Order struct {
	ID        string    `json:"id"`
	Item      string    `json:"item"`
	Quantity  int       `json:"quantity"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

// CreateOrderRequest is what the client sends via POST /orders
type CreateOrderRequest struct {
	Item     string `json:"item"`
	Quantity int    `json:"quantity"`
}

// OrderEvent is what we publish to Kafka
// It contains the full order data plus an event type
type OrderEvent struct {
	EventType string `json:"event_type"` // "order.created", "order.cancelled", etc.
	Order     Order  `json:"order"`
}

// Stats is the response shape for GET /api/stats
type Stats struct {
	TotalOrders  int64            `json:"total_orders"`
	FailedOrders int64            `json:"failed_orders"`
	ItemsSold    map[string]int64 `json:"items_sold"`
}
