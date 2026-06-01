package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/niphitphon8bit/kafka-order-lab/internal/config"
	"github.com/niphitphon8bit/kafka-order-lab/internal/inventory"
	"github.com/niphitphon8bit/kafka-order-lab/internal/infrastructure/kafka"
	"github.com/niphitphon8bit/kafka-order-lab/internal/infrastructure/store"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.LoadInventoryServiceConfig()

	// Infrastructure layer
	redisClient, err := store.NewClient(cfg.RedisAddr)
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
	defer redisClient.Close()

	// Service layer — store.Client satisfies inventory.StockRepository via duck typing
	svc := inventory.NewStockService(redisClient)

	initialStock := map[string]int{
		"laptop":   10,
		"mouse":    50,
		"keyboard": 30,
		"monitor":  15,
		"headset":  25,
	}
	if err := svc.InitStock(ctx, initialStock); err != nil {
		log.Fatalf("Failed to initialize stock: %v", err)
	}

	stocks, _ := svc.GetAllStock(ctx)
	log.Println("=== Current Stock (from Redis) ===")
	for item, qty := range stocks {
		log.Printf("  %s: %d", item, qty)
	}

	// Handler layer
	grpcServer, err := inventory.StartGRPC(cfg.GRPCPort, svc)
	if err != nil {
		log.Fatalf("Failed to start gRPC server: %v", err)
	}
	defer grpcServer.GracefulStop()

	kafkaHandler := inventory.NewKafkaHandler(svc)

	consumer, err := kafka.NewConsumerGroup(
		[]string{cfg.KafkaBrokers},
		cfg.KafkaGroupID,
		[]string{cfg.KafkaTopic},
		kafkaHandler.Handle,
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	if err := consumer.Start(ctx); err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
}
