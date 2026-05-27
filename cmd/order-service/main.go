package main

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/niphitphon8bit/kafka-order-lab/internal/config"
	"github.com/niphitphon8bit/kafka-order-lab/internal/kafka"
	"github.com/niphitphon8bit/kafka-order-lab/internal/models"
)

var producer *kafka.Producer

func main() {
	// ---- Load config from environment variables ----
	cfg := config.LoadOrderServiceConfig()

	// ---- Connect to Kafka ----
	var err error
	producer, err = kafka.NewProducer(
		[]string{cfg.KafkaBrokers},
		cfg.KafkaTopic,
	)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// ---- HTTP Routes ----
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("POST /orders", handleCreateOrder)

	log.Printf("Order service starting on :%s", cfg.HTTPPort)
	if err := http.ListenAndServe(":"+cfg.HTTPPort, mux); err != nil {
		log.Fatal(err)
	}
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"service": "order-service",
	})
}

func handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	var req models.CreateOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request body"}`, http.StatusBadRequest)
		return
	}

	if req.Item == "" || req.Quantity <= 0 {
		http.Error(w, `{"error":"item and quantity are required"}`, http.StatusBadRequest)
		return
	}

	order := models.Order{
		ID:        uuid.New().String(),
		Item:      req.Item,
		Quantity:  req.Quantity,
		Status:    "created",
		CreatedAt: time.Now(),
	}

	event := models.OrderEvent{
		EventType: "order.created",
		Order:     order,
	}

	_, _, err := producer.SendMessage(order.ID, event)
	if err != nil {
		log.Printf("Failed to publish order event: %v", err)
		http.Error(w, `{"error":"failed to process order"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(order)
}
