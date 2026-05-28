# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Module

`github.com/niphitphon8bit/kafka-order-lab`

## Commands

```bash
# Start infrastructure (Kafka, Redis, Kafka UI)
docker compose -f deployments/docker-compose.yml up -d

# Run inventory service first (it exposes gRPC on :50051 that order-service needs)
go run ./cmd/inventory-service/main.go

# Run order service — HTTP :8080, connects to inventory gRPC :50051
go run ./cmd/order-service/main.go

# Build all binaries
go build ./...

# Run tests
go test ./...

# Regenerate protobuf Go code after editing proto/inventory.proto
protoc --go_out=. --go_opt=paths=source_relative \
       --go-grpc_out=. --go-grpc_opt=paths=source_relative \
       proto/inventory.proto
# Output goes to internal/pb/

# Create an order (end-to-end test)
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"item":"laptop","quantity":1,"user_id":"user-123"}'
```

Kafka UI: `http://localhost:8090` — Dashboard: `http://localhost:8080/static/`

## Architecture

Hybrid synchronous + asynchronous flow:

```
Client → Order Service (HTTP :8080)
              │
              ├─ gRPC → Inventory Service (:50051)   ← synchronous stock check
              │         CheckStock: "do you have 1 laptop?"
              │
              └─ Kafka topic "orders"                ← async after stock confirmed
                        │
                        └─ Inventory Service (consumer group "inventory-service")
                                  └─ deducts stock in Redis + updates analytics
```

**Order Service** (`cmd/order-service/`) — HTTP producer. On `POST /orders`:
1. Calls `CheckStock` via gRPC to reject orders immediately if stock is insufficient (409).
2. Publishes `OrderEvent{Type: "order.created"}` to Kafka with order UUID as message key.
3. Also serves `GET /api/stocks` and `GET /api/stats` by reading Redis directly.
4. Embeds and serves `static/index.html` as a web dashboard (`//go:embed static`).

**Inventory Service** (`cmd/inventory-service/`) — dual role:
- **gRPC server** on `:50051` — answers `CheckStock` / `GetStock` calls from order-service.
- **Kafka consumer** — on each `order.created` event: idempotency check → atomic stock deduction → analytics counters.

**Shared packages under `internal/`:**
- `config/` — `LoadOrderServiceConfig` / `LoadInventoryServiceConfig`; reads env vars, falls back to localhost defaults.
- `grpc/server.go` — gRPC server wrapping Redis stock logic; `grpc/client.go` — client used by order-service. **Note:** package is named `grpc`, aliased as `appgrpc` at import sites to avoid collision with `google.golang.org/grpc`.
- `kafka/` — Sarama producer and consumer group wrappers.
- `models/order.go` — `Order`, `CreateOrderRequest`, `OrderEvent` shared structs.
- `pb/` — generated protobuf code; do not edit manually, regenerate with `protoc`.
- `redis/` — connection client + all stock/idempotency/analytics operations.

## Key env vars (with defaults)

| Var | Default | Used by |
|-----|---------|---------|
| `KAFKA_BROKERS` | `localhost:9092` | both |
| `KAFKA_TOPIC` | `orders` | both |
| `REDIS_ADDR` | `localhost:6379` | both |
| `INVENTORY_GRPC_ADDR` | `localhost:50051` | order-service |
| `GRPC_PORT` | `50051` | inventory-service |
| `HTTP_PORT` | `8080` | order-service |

Inside Docker containers use `kafka:19092` and `redis:6379`.

## Infrastructure

Kafka runs in KRaft mode (no Zookeeper). Redis runs with AOF persistence. Inventory service seeds initial stock on startup (laptop×10, mouse×50, keyboard×30, monitor×15, headset×25) — restarting resets stock to these values.

## Key dependencies

- `github.com/IBM/sarama` — Kafka client
- `github.com/google/uuid` — order ID generation
- `github.com/redis/go-redis/v9` — Redis client
- `google.golang.org/grpc` — gRPC framework
- `google.golang.org/protobuf` — protobuf runtime
