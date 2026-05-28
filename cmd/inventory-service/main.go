package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"syscall"
	"time"

	"github.com/niphitphon8bit/kafka-order-lab/internal/config"
	appgrpc "github.com/niphitphon8bit/kafka-order-lab/internal/grpc"
	"github.com/niphitphon8bit/kafka-order-lab/internal/kafka"
	"github.com/niphitphon8bit/kafka-order-lab/internal/models"
	appredis "github.com/niphitphon8bit/kafka-order-lab/internal/redis"
)

var redisClient *appredis.Client

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.LoadInventoryServiceConfig()

	// ---- Connect to Redis ----
	var err error
	redisClient, err = appredis.NewClient(cfg.RedisAddr)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisClient.Close()

	// ---- Initialize stock ----
	initialStock := map[string]int{
		"laptop":   10,
		"mouse":    50,
		"keyboard": 30,
		"monitor":  15,
		"headset":  25,
	}
	if err := redisClient.InitStock(ctx, initialStock); err != nil {
		log.Fatalf("Failed to initialize stock: %v", err)
	}

	stock, _ := redisClient.GetAllStock(ctx)
	log.Println("=== Current Stock (from Redis) ===")
	for item, qty := range stock {
		log.Printf("  %s: %d", item, qty)
	}

	// ---- Start gRPC server ----
	// This runs in a goroutine — Order Service can call CheckStock via gRPC
	grpcServer, err := appgrpc.Start(cfg.GRPCPort, redisClient)
	if err != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}
	defer grpcServer.GracefulStop()

	// ---- Start Kafka consumer ----
	consumer, err := kafka.NewConsumerGroup(
		[]string{cfg.KafkaBrokers},
		cfg.KafkaGroupID,
		[]string{cfg.KafkaTopic},
		handleOrderEvent,
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// ---- Start consuming (blocks until shutdown) ----
	if err := consumer.Start(ctx); err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
}

func handleOrderEvent(key string, value []byte) error {
	var event models.OrderEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	switch event.EventType {
	case "order.created":
		return processOrderCreated(event.Order)
	default:
		log.Printf("Unknown event type: %s, skipping", event.EventType)
		return nil
	}
}

func processOrderCreated(order models.Order) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	processed, err := redisClient.IsProcessed(ctx, order.ID)
	if err != nil {
		return fmt.Errorf("idempotency check failed: %w", err)
	}
	if processed {
		log.Printf("⏭️  Order %s already processed, skipping", order.ID)
		return nil
	}

	newQty, err := redisClient.CheckAndDeductStock(ctx, order.Item, order.Quantity)
	if err != nil {
		log.Printf("❌ Order %s: %v", order.ID, err)
		redisClient.IncrementCounter(ctx, "failed_orders", 1)
		return nil
	}

	if err := redisClient.MarkProcessed(ctx, order.ID, 24*time.Hour); err != nil {
		log.Printf("Warning: failed to mark order %s as processed: %v", order.ID, err)
	}

	redisClient.IncrementCounter(ctx, "total_orders", 1)
	redisClient.IncrementCounter(ctx, fmt.Sprintf("items:%s", order.Item), int64(order.Quantity))

	log.Printf("✅ Order %s: reserved %d x '%s' (stock remaining: %d)",
		order.ID, order.Quantity, order.Item, newQty)

	return nil
}
