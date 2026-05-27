# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Module

`github.com/niphitphon8bit/kafka-order-lab`

## Commands

```bash
# Start infrastructure (Kafka, Redis, Kafka UI)
docker compose -f deployments/docker-compose.yml up -d

# Run order service (producer) — port 8080
go run ./cmd/order-service/main.go

# Run inventory service (consumer)
go run ./cmd/inventory-service/main.go

# Build all binaries
go build ./...

# Run tests
go test ./...

# Run a single package's tests
go test ./internal/kafka/...

# Create an order (test the flow end-to-end)
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"item":"laptop","quantity":1,"user_id":"user-123"}'
```

Kafka UI is available at `http://localhost:8090` when Docker is running.

## Architecture

Event-driven microservices communicating over Kafka, with Redis for distributed state.

```
Client → Order Service (HTTP :8080) → Kafka topic "orders" → Inventory Service
                                                            → Notification Service (not yet implemented)
```

**Order Service** (`cmd/order-service/`) — HTTP producer. Accepts `POST /orders`, wraps the request in an `OrderEvent{Type: "order.created"}`, and publishes it to Kafka using the order's UUID as the message key (guarantees per-order ordering within a partition).

**Inventory Service** (`cmd/inventory-service/`) — Kafka consumer group `"inventory-service"`. On each `order.created` event it:
1. Checks idempotency via Redis key `processed:{orderID}` (24h TTL) — skips duplicate deliveries.
2. Atomically deducts stock from Redis HASH `stock:{item}` using `HINCRBY` with a negative delta.
3. Records analytics in `stats:{counter_name}` string keys (`total_orders`, `failed_orders`, `items:{item}`).

**Shared packages** under `internal/`:
- `kafka/producer.go` — Sarama sync producer wrapper; JSON-encodes messages with a string key.
- `kafka/consumer.go` — Sarama consumer group wrapper; routes messages to a handler `func([]byte) error`.
- `redis/client.go` — Redis connection; `redis/stock.go` contains all stock and idempotency logic.
- `models/order.go` — `Order`, `CreateOrderRequest`, `OrderEvent` structs shared by both services.

## Infrastructure

Kafka runs in KRaft mode (no Zookeeper). Host applications connect on `localhost:9092`; services inside Docker use `kafka:19092`.

Redis runs with AOF persistence. Inventory service seeds initial stock on startup (laptop×10, mouse×50, keyboard×30, monitor×15, headset×25) — re-running the service resets these values.

## Key dependencies

- `github.com/IBM/sarama` — Kafka client (producer + consumer group)
- `github.com/google/uuid` — order ID generation
- `github.com/redis/go-redis/v9` — Redis client (check go.mod; currently listed as indirect)
