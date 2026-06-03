# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Module

`github.com/niphitphon8bit/kafka-order-lab`

## Commands

```bash
# Prefer make targets — see Makefile for all available commands
make up            # start infrastructure (Kafka, Redis, Kafka UI)
make run-inventory # run inventory service locally
make run-order     # run order service locally
make test          # run all tests
make build         # compile binaries to bin/
make order         # send a test POST /orders

# Regenerate protobuf Go code after editing proto/inventory.proto
# Output path is controlled by go_package in the proto file → internal/infrastructure/pb/
make proto

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
                                  └─ atomic Lua stock deduction + analytics
```

Each service follows **Handler → Service → Repository** (clean architecture):

```
cmd/{service}/main.go     — wiring only (connects infrastructure to domain)
internal/{service}/       — domain logic (handler, service, repository interface)
internal/infrastructure/  — technical implementations (Redis, Kafka, gRPC, protobuf)
```

**Order Service** (`cmd/order-service/`) — HTTP API. On `POST /orders`:
1. `order.OrderHandler` decodes request, calls `order.OrderService`
2. `order.orderService` calls `CheckStock` via gRPC → rejects 409 if insufficient
3. Publishes `OrderEvent{type: "order.created"}` to Kafka with order UUID as key
4. `GET /api/stocks` and `GET /api/stats` read Redis via `StockReader` interface

**Inventory Service** (`cmd/inventory-service/`) — dual role:
- **gRPC server** on `:50051` — `inventory.GRPCHandler` answers `CheckStock`/`GetStock` via `StockService`
- **Kafka consumer** — `inventory.KafkaHandler` on each `order.created`: idempotency check → atomic Lua stock deduction → analytics counters

## Package Layout

```
internal/
  order/                  — order domain (handler, service, interfaces)
  inventory/              — inventory domain (handler, service, repository interface)
  models/                 — shared types: Order, OrderEvent, Stats
  config/                 — env-var config loader
  infrastructure/
    store/                — Redis implementation of inventory.StockRepository
    inventoryrpc/         — gRPC client (used by order-service to call inventory)
    kafka/                — Sarama producer and consumer group wrappers
    pb/                   — generated protobuf code (do not edit manually)
```

**Key interfaces:**
- `inventory.StockRepository` — implemented by `*infrastructure/store.Client` (duck typing)
- `inventory.StockService` — used by both `inventory.GRPCHandler` and `inventory.KafkaHandler`
- `order.OrderService` — used by `order.OrderHandler`
- `order.StockReader`, `order.InventoryChecker`, `order.EventPublisher` — narrow deps injected into `order.orderService`

## Key env vars (with defaults)

| Var | Default | Used by |
|-----|---------|---------|
| `KAFKA_BROKERS` | `localhost:9092` | both |
| `KAFKA_TOPIC` | `orders` | both |
| `REDIS_ADDR` | `localhost:6379` | both |
| `INVENTORY_GRPC_ADDR` | `localhost:50051` | order-service |
| `GRPC_PORT` | `50051` | inventory-service |
| `HTTP_PORT` | `8080` | order-service |
| `KAFKA_GROUP_ID` | `inventory-service` | inventory-service |

Inside Docker containers use `kafka:19092` and `redis:6379`.

## Infrastructure Notes

- Kafka runs in KRaft mode (no Zookeeper). Redis runs with AOF persistence.
- Inventory service seeds initial stock on startup using `HSETNX` (set-only-if-not-exists) — restarting the service does **not** reset stock. To reset, restart Redis (`make down && make up`).
- `CheckAndDeductStock` uses a Lua script for atomic check+deduct (no TOCTOU race).
- `GetAllStock` and `GetAllItemStats` use cursor-based `SCAN` (never `KEYS`).

## Key dependencies

- `github.com/IBM/sarama` — Kafka client
- `github.com/google/uuid` — order ID generation
- `github.com/redis/go-redis/v9` — Redis client
- `google.golang.org/grpc` — gRPC framework
- `google.golang.org/protobuf` — protobuf runtime
