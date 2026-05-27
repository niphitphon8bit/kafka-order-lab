package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/niphitphon8bit/kafka-order-lab/internal/kafka"
	"github.com/niphitphon8bit/kafka-order-lab/internal/models"
)

// Global producer instance (in a real app, you'd use dependency injection)
var producer *kafka.Producer

func main() {
	// ---- Connect to Kafka ----
	var err error
	producer, err = kafka.NewProducer(
		[]string{"localhost:9092"}, // Kafka broker address
		"orders",                   // Topic name
	)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// ---- HTTP Routes ----
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("POST /orders", handleCreateOrder)

	log.Println("Order service starting on :8080")
	if err := http.ListenAndServe(":8080", mux); err != nil {
		log.Fatal(err)
	}
}

// handleHealth returns service status
func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"service": "order-service",
	})
}

// handleCreateOrder:
//  1. Parse the request body
//  2. Create an Order with a unique ID
//  3. Publish an "order.created" event to Kafka
//  4. Return the created order to the client
func handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	// Step 1: Parse request body
	var req models.CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	// Validate
	if req.Item == "" || req.Quantity <= 0 {
		http.Error(w, `{"error":"item and quantity are required"}`, http.StatusBadRequest)
		return
	}

	// Step 2: Create the order
	order := models.Order{
		ID:        uuid.New().String(),
		Item:      req.Item,
		Quantity:  req.Quantity,
		Status:    "created",
		CreatedAt: time.Now(),
	}

	// Step 3: Publish event to Kafka
	event := models.OrderEvent{
		EventType: "order.created",
		Order:     order,
	}

	// Use order ID as the key — all events for the same order
	// will go to the same partition (guarantees ordering per order)
	_, _, err := producer.SendMessage(order.ID, event)
	if err != nil {
		log.Printf("Failed to publish order event: %v", err)
		http.Error(w, `{"error":"failed to process order"}`, http.StatusInternalServerError)
		return
	}

	// Step 4: Return the created order
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(order)
}
