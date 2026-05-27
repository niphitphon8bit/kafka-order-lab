package kafka

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/IBM/sarama"
)

// Producer wraps the Sarama sync producer
type Producer struct {
	producer sarama.SyncProducer
	topic    string
}

// NewProducer creates a new Kafka producer connected to the given brokers.
//
// How it works:
//   1. Configure Sarama (the Kafka client library)
//   2. Connect to the Kafka broker(s)
//   3. Return a Producer that can send messages
//
// SyncProducer = waits for Kafka to acknowledge each message (safer, simpler)
// AsyncProducer = fire-and-forget (faster, but you handle errors separately)
// We use Sync for learning — it's easier to understand the flow.
func NewProducer(brokers []string, topic string) (*Producer, error) {
	// Sarama config — controls how the producer behaves
	config := sarama.NewConfig()

	// RequiredAcks: how many brokers must confirm receipt?
	//   WaitForAll  = all replicas must confirm (safest, slowest)
	//   WaitForLocal = only the leader confirms (faster, less safe)
	//   NoResponse  = don't wait at all (fastest, least safe)
	config.Producer.RequiredAcks = sarama.WaitForAll

	// Retry up to 5 times if sending fails
	config.Producer.Retry.Max = 5

	// Return successes — required for SyncProducer
	config.Producer.Return.Successes = true

	// Connect to Kafka broker(s)
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	log.Printf("Kafka producer connected to %v, topic: %s", brokers, topic)
	return &Producer{
		producer: producer,
		topic:    topic,
	}, nil
}

// SendMessage serializes the data to JSON and sends it to Kafka.
//
// Parameters:
//   - key: determines which partition the message goes to.
//          Messages with the same key always go to the same partition.
//          This guarantees ordering for that key.
//          Example: use order ID as key → all events for order "abc" are in order.
//   - value: the actual data (will be JSON-encoded)
//
// Returns: partition number and offset (position) where the message was stored
func (p *Producer) SendMessage(key string, value any) (int32, int64, error) {
	// Serialize value to JSON bytes
	jsonBytes, err := json.Marshal(value)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to marshal message: %w", err)
	}

	// Build the Kafka message
	msg := &sarama.ProducerMessage{
		Topic: p.topic,
		Key:   sarama.StringEncoder(key),     // Key as string
		Value: sarama.ByteEncoder(jsonBytes),  // Value as JSON bytes
	}

	// Send and wait for acknowledgment
	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to send message: %w", err)
	}

	log.Printf("Message sent: topic=%s partition=%d offset=%d key=%s",
		p.topic, partition, offset, key)

	return partition, offset, nil
}

// Close shuts down the producer connection
func (p *Producer) Close() error {
	return p.producer.Close()
}
