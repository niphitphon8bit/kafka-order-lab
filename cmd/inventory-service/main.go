package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os/signal"
	"sync"
	"syscall"

	"github.com/niphitphon8bit/kafka-order-lab/internal/kafka"
	"github.com/niphitphon8bit/kafka-order-lab/internal/models"
)

// In-memory stock database (will replace with Redis in Step 6)
var (
	stock = map[string]int{
		"laptop":   10,
		"mouse":    50,
		"keyboard": 30,
	}
	stockMu sync.Mutex // Protect concurrent access to the map
)

func main() {
	// ---- Graceful shutdown ----
	// Create a context that cancels on SIGINT (Ctrl+C) or SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ---- Create consumer ----
	consumer, err := kafka.NewConsumerGroup(
		[]string{"localhost:9092"}, // Kafka broker
		"inventory-service",        // Consumer group ID
		[]string{"orders"},         // Topics to subscribe to
		handleOrderEvent,           // Our message handler function
	)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Print initial stock
	log.Println("=== Initial Stock ===")
	for item, qty := range stock {
		log.Printf("  %s: %d", item, qty)
	}

	// ---- Start consuming (blocks until ctx is cancelled) ----
	if err := consumer.Start(ctx); err != nil {
		log.Fatalf("Consumer error: %v", err)
	}
}

// handleOrderEvent is our business logic — called for each Kafka message.
//
// This function:
//  1. Deserializes the JSON message into an OrderEvent
//  2. Checks the event type
//  3. Processes the order (deduct stock)
//  4. Logs the result
func handleOrderEvent(key string, value []byte) error {
	// Step 1: Parse the event
	var event models.OrderEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	// Step 2: Route by event type
	switch event.EventType {
	case "order.created":
		return processOrderCreated(event.Order)
	default:
		log.Printf("Unknown event type: %s, skipping", event.EventType)
		return nil
	}
}

// processOrderCreated deducts stock for the ordered item.
func processOrderCreated(order models.Order) error {
	stockMu.Lock()
	defer stockMu.Unlock()

	// Check if item exists
	currentStock, exists := stock[order.Item]
	if !exists {
		log.Printf("❌ Order %s: item '%s' not found in inventory", order.ID, order.Item)
		return nil // Don't return error — we processed it, just can't fulfill
	}

	// Check if enough stock
	if currentStock < order.Quantity {
		log.Printf("❌ Order %s: insufficient stock for '%s' (need %d, have %d)",
			order.ID, order.Item, order.Quantity, currentStock)
		return nil
	}

	// Deduct stock
	stock[order.Item] = currentStock - order.Quantity

	log.Printf("✅ Order %s: reserved %d x '%s' (stock: %d → %d)",
		order.ID, order.Quantity, order.Item, currentStock, stock[order.Item])

	return nil
}
