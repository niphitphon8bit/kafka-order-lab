package main

import (
	"embed"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/niphitphon8bit/kafka-order-lab/internal/config"
	"github.com/niphitphon8bit/kafka-order-lab/internal/kafka"
	"github.com/niphitphon8bit/kafka-order-lab/internal/models"
	appredis "github.com/niphitphon8bit/kafka-order-lab/internal/redis"
)

//go:embed static
var staticFiles embed.FS

var (
	producer    *kafka.Producer
	redisClient *appredis.Client
)

func main() {
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

	// ---- Connect to Redis (read-only, for dashboard) ----
	redisClient, err = appredis.NewClient(cfg.RedisAddr)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisClient.Close()

	// ---- HTTP Routes ----
	mux := http.NewServeMux()

	// API
	mux.HandleFunc("GET /health", handleHealth)
	mux.HandleFunc("POST /orders", handleCreateOrder)
	mux.HandleFunc("GET /api/stocks", handleGetStocks)
	mux.HandleFunc("GET /api/stats", handleGetStats)

	// Dashboard — serve static HTML
	mux.Handle("/", http.FileServer(http.FS(staticFiles)))

	log.Printf("Order service starting on :%s", cfg.HTTPPort)
	log.Printf("Dashboard: http://localhost:%s/static/", cfg.HTTPPort)
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

func handleGetStocks(w http.ResponseWriter, r *http.Request) {
	stocks, err := redisClient.GetAllStock(r.Context())
	if err != nil {
		http.Error(w, `{"error":"failed to get stocks"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stocks)
}

func handleGetStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	totalOrders, _ := redisClient.GetCounter(ctx, "total_orders")
	failedOrders, _ := redisClient.GetCounter(ctx, "failed_orders")
	laptops, _ := redisClient.GetCounter(ctx, "items:laptop")
	mice, _ := redisClient.GetCounter(ctx, "items:mouse")
	keyboards, _ := redisClient.GetCounter(ctx, "items:keyboard")
	monitors, _ := redisClient.GetCounter(ctx, "items:monitor")
	headsets, _ := redisClient.GetCounter(ctx, "items:headset")

	stats := map[string]any{
		"total_orders":  totalOrders,
		"failed_orders": failedOrders,
		"items_sold": map[string]int64{
			"laptop":   laptops,
			"mouse":    mice,
			"keyboard": keyboards,
			"monitor":  monitors,
			"headset":  headsets,
		},
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}
