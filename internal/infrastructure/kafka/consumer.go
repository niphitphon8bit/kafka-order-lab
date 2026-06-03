package kafka

import (
	"context"
	"log"

	"github.com/IBM/sarama"
)

// MessageHandler is a function that processes a Kafka message.
// Your business logic goes here.
// Return nil to mark the message as processed (commit offset).
// Return error to log the failure (message won't be reprocessed unless you handle that).
type MessageHandler func(key string, value []byte) error

// ConsumerGroup wraps Sarama's consumer group.
//
// Why Consumer GROUP (not just consumer)?
//
//   Consumer:        One process reads ALL partitions alone
//   Consumer Group:  Multiple processes SHARE the partitions
//
//   Topic "orders" has 2 partitions:
//
//   Without group (1 consumer):
//     Consumer A reads: [P0] + [P1]  ← does all the work
//
//   With group (2 consumers, same group "inventory"):
//     Consumer A reads: [P0]   ← splits the work
//     Consumer B reads: [P1]   ← automatic rebalancing!
//
//   With 2 groups (different services):
//     Group "inventory":  reads ALL messages (for stock)
//     Group "notification": reads ALL messages (for emails)
//     ← each group gets its own copy
type ConsumerGroup struct {
	group   sarama.ConsumerGroup
	handler *consumerHandler
	topics  []string
}

// NewConsumerGroup creates a consumer that belongs to a group.
//
// Parameters:
//   - brokers: Kafka broker addresses
//   - groupID: consumer group name (e.g., "inventory-service")
//     All instances with the same groupID share the work.
//   - topics: which topics to subscribe to
//   - handler: your function that processes each message
func NewConsumerGroup(brokers []string, groupID string, topics []string, handler MessageHandler) (*ConsumerGroup, error) {
	config := sarama.NewConfig()

	// OffsetNewest = only read NEW messages (ignore old ones)
	// OffsetOldest = read from the beginning (process everything)
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Create the consumer group
	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		return nil, err
	}

	log.Printf("Consumer group [%s] connected to %v, topics: %v", groupID, brokers, topics)

	return &ConsumerGroup{
		group:   group,
		handler: &consumerHandler{handler: handler},
		topics:  topics,
	}, nil
}

// Start begins consuming messages in a blocking loop.
// It runs until the context is cancelled.
//
// The loop is needed because Sarama's Consume() returns when a
// rebalance happens (e.g., another consumer joins/leaves the group).
// After rebalance, we call Consume() again to continue processing.
func (c *ConsumerGroup) Start(ctx context.Context) error {
	log.Println("Consumer started, waiting for messages...")

	for {
		// Consume() blocks until rebalance or context cancel
		err := c.group.Consume(ctx, c.topics, c.handler)
		if err != nil {
			log.Printf("Consumer error: %v", err)
		}

		// Check if context was cancelled (shutdown signal)
		if ctx.Err() != nil {
			log.Println("Consumer stopped")
			return nil
		}
	}
}

// Close shuts down the consumer group
func (c *ConsumerGroup) Close() error {
	return c.group.Close()
}

// ---- consumerHandler implements sarama.ConsumerGroupHandler ----
// Sarama requires this interface to process messages.
// It has 3 methods that represent the consumer lifecycle:
//
//   Setup()    → called when partitions are assigned (after rebalance)
//   Cleanup()  → called when partitions are revoked (before rebalance)
//   ConsumeClaim() → called for each partition, processes messages in a loop
type consumerHandler struct {
	handler MessageHandler
}

// Setup runs when this consumer is assigned partitions
func (h *consumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	log.Printf("Consumer partitions assigned: %v", session.Claims())
	return nil
}

// Cleanup runs when partitions are being revoked (rebalance)
func (h *consumerHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	log.Println("Consumer partitions revoked, cleaning up...")
	return nil
}

// ConsumeClaim processes messages from a single partition.
// Sarama calls this once per assigned partition, in its own goroutine.
func (h *consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	// claim.Messages() is a channel that delivers messages one by one
	for msg := range claim.Messages() {
		log.Printf("Received: topic=%s partition=%d offset=%d key=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key))

		// Call our business logic handler
		if err := h.handler(string(msg.Key), msg.Value); err != nil {
			log.Printf("Error processing message: %v", err)
			// In production, you might: retry, send to dead-letter queue, etc.
			continue
		}

		// Mark message as processed (commit offset)
		// This tells Kafka: "I've handled this message, don't send it again"
		session.MarkMessage(msg, "")
	}
	return nil
}
