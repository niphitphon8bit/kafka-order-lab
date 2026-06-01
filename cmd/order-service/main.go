package main

import (
	"embed"
	"log"
	"net/http"

	"github.com/niphitphon8bit/kafka-order-lab/internal/config"
	"github.com/niphitphon8bit/kafka-order-lab/internal/infrastructure/inventoryrpc"
	"github.com/niphitphon8bit/kafka-order-lab/internal/infrastructure/kafka"
	"github.com/niphitphon8bit/kafka-order-lab/internal/infrastructure/store"
	"github.com/niphitphon8bit/kafka-order-lab/internal/order"
)

//go:embed static
var staticFiles embed.FS

func main() {
	cfg := config.LoadOrderServiceConfig()

	// Infrastructure layer
	redisClient, err := store.NewClient(cfg.RedisAddr)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisClient.Close()

	grpcClient, err := inventoryrpc.NewInventoryClient(cfg.InventoryGRPCAddr)
	if err != nil {
		log.Fatalf("Failed to connect to inventory gRPC: %v", err)
	}
	defer grpcClient.Close()

	producer, err := kafka.NewProducer([]string{cfg.KafkaBrokers}, cfg.KafkaTopic)
	if err != nil {
		log.Fatalf("Failed to create Kafka producer: %v", err)
	}
	defer producer.Close()

	// Service layer
	svc := order.NewOrderService(redisClient, grpcClient, producer)

	// Handler layer
	h := order.NewOrderHandler(svc)

	mux := http.NewServeMux()
	h.RegisterRoutes(mux)
	mux.Handle("/", http.FileServer(http.FS(staticFiles)))

	log.Printf("Order service starting on :%s", cfg.HTTPPort)
	log.Printf("Dashboard: http://localhost:%s/static/", cfg.HTTPPort)
	if err := http.ListenAndServe(":"+cfg.HTTPPort, mux); err != nil {
		log.Fatal(err)
	}
}
