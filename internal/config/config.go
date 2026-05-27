package config

import "os"

// getEnv returns the value of an environment variable,
// or a default value if it's not set.
//
// This lets us:
//   - Run locally:   uses defaults (localhost:9092, localhost:6379)
//   - Run in Docker: set env vars to container names (kafka:19092, redis:6379)
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// OrderServiceConfig returns config for the Order Service
type OrderServiceConfig struct {
	KafkaBrokers string
	KafkaTopic   string
	HTTPPort     string
	RedisAddr    string
}

func LoadOrderServiceConfig() OrderServiceConfig {
	return OrderServiceConfig{
		KafkaBrokers: getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "orders"),
		HTTPPort:     getEnv("HTTP_PORT", "8080"),
		RedisAddr:    getEnv("REDIS_ADDR", "localhost:6379"),
	}
}

// InventoryServiceConfig returns config for the Inventory Service
type InventoryServiceConfig struct {
	KafkaBrokers   string
	KafkaTopic     string
	KafkaGroupID   string
	RedisAddr      string
}

func LoadInventoryServiceConfig() InventoryServiceConfig {
	return InventoryServiceConfig{
		KafkaBrokers: getEnv("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:   getEnv("KAFKA_TOPIC", "orders"),
		KafkaGroupID: getEnv("KAFKA_GROUP_ID", "inventory-service"),
		RedisAddr:    getEnv("REDIS_ADDR", "localhost:6379"),
	}
}
