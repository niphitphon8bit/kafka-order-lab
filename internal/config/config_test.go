package config

import (
	"os"
	"testing"
)

func TestLoadOrderServiceConfig_Defaults(t *testing.T) {
	// Clear any env vars that might be set in the environment
	for _, k := range []string{"KAFKA_BROKERS", "KAFKA_TOPIC", "HTTP_PORT", "REDIS_ADDR", "INVENTORY_GRPC_ADDR"} {
		os.Unsetenv(k)
	}

	cfg := LoadOrderServiceConfig()

	if cfg.KafkaBrokers != "localhost:9092" {
		t.Errorf("KafkaBrokers: got %q, want %q", cfg.KafkaBrokers, "localhost:9092")
	}
	if cfg.KafkaTopic != "orders" {
		t.Errorf("KafkaTopic: got %q, want %q", cfg.KafkaTopic, "orders")
	}
	if cfg.HTTPPort != "8080" {
		t.Errorf("HTTPPort: got %q, want %q", cfg.HTTPPort, "8080")
	}
	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr: got %q, want %q", cfg.RedisAddr, "localhost:6379")
	}
	if cfg.InventoryGRPCAddr != "localhost:50051" {
		t.Errorf("InventoryGRPCAddr: got %q, want %q", cfg.InventoryGRPCAddr, "localhost:50051")
	}
}

func TestLoadOrderServiceConfig_EnvOverride(t *testing.T) {
	os.Setenv("KAFKA_BROKERS", "kafka:19092")
	os.Setenv("HTTP_PORT", "9090")
	defer func() {
		os.Unsetenv("KAFKA_BROKERS")
		os.Unsetenv("HTTP_PORT")
	}()

	cfg := LoadOrderServiceConfig()

	if cfg.KafkaBrokers != "kafka:19092" {
		t.Errorf("KafkaBrokers: got %q, want %q", cfg.KafkaBrokers, "kafka:19092")
	}
	if cfg.HTTPPort != "9090" {
		t.Errorf("HTTPPort: got %q, want %q", cfg.HTTPPort, "9090")
	}
	// Unset vars still fall back to defaults
	if cfg.RedisAddr != "localhost:6379" {
		t.Errorf("RedisAddr: got %q, want %q", cfg.RedisAddr, "localhost:6379")
	}
}

func TestLoadInventoryServiceConfig_Defaults(t *testing.T) {
	for _, k := range []string{"KAFKA_BROKERS", "KAFKA_TOPIC", "KAFKA_GROUP_ID", "REDIS_ADDR", "GRPC_PORT"} {
		os.Unsetenv(k)
	}

	cfg := LoadInventoryServiceConfig()

	if cfg.KafkaBrokers != "localhost:9092" {
		t.Errorf("KafkaBrokers: got %q, want %q", cfg.KafkaBrokers, "localhost:9092")
	}
	if cfg.KafkaGroupID != "inventory-service" {
		t.Errorf("KafkaGroupID: got %q, want %q", cfg.KafkaGroupID, "inventory-service")
	}
	if cfg.GRPCPort != "50051" {
		t.Errorf("GRPCPort: got %q, want %q", cfg.GRPCPort, "50051")
	}
}
