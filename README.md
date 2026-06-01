# kafka-order-lab

A hands-on learning project for building event-driven microservices with **Go**, **Kafka**, **Redis**, **gRPC**, and **Docker**.

## Architecture

```
  HTTP Client
      │
      ▼
┌─────────────────────────────────────┐
│          Order Service (:8080)      │
│                                     │
│  Handler → Service → (interfaces)   │
│                                     │
│  POST /orders:                      │
│  1. CheckStock ──gRPC──────────────►│
│  2. Publish to Kafka ───────────────┼──────────────────────┐
│                                     │                      │
│  GET /api/stocks, /api/stats:       │                      │
│     reads Redis directly            │                      │
└─────────────────────────────────────┘                      │
          │  gRPC (:50051)                                   │
          ▼                                                  ▼
┌─────────────────────────────────────┐       ┌─────────────────────────┐
│       Inventory Service             │       │   Kafka topic: "orders" │
│                                     │       └────────────┬────────────┘
│  Handler → Service → Repository     │                    │ consumer group
│                                     │◄───────────────────┘
│  gRPC server: CheckStock/GetStock   │
│  Kafka consumer: process order      │
│    1. Idempotency check             │       ┌─────────────────────────┐
│    2. Atomic stock deduction (Lua)  │◄─────►│         Redis           │
│    3. Update analytics counters     │       │  stock:{item} HASH      │
└─────────────────────────────────────┘       │  processed:{id} TTL     │
                                              │  stats:total_orders     │
                                              │  stats:items:{item}     │
                                              └─────────────────────────┘
```

**Flow for `POST /orders`:**
1. Order Service calls Inventory Service via **gRPC** (sync) — rejects with 409 if stock is insufficient
2. On success, publishes `order.created` to **Kafka** (async)
3. Inventory Service Kafka consumer processes the event: idempotency check → **atomic Lua** stock deduction → analytics update

## Tech Stack

| Technology | Version | Role |
|------------|---------|------|
| **Go** | 1.26 | Microservices |
| **Apache Kafka** | 3.9 (KRaft) | Async event bus — no Zookeeper |
| **Redis** | 7 | Stock storage, idempotency, analytics |
| **gRPC / Protobuf** | — | Synchronous inter-service communication |
| **Docker Compose** | — | Local infrastructure orchestration |

## Project Structure

```
kafka-order-lab/
├── cmd/
│   ├── order-service/
│   │   ├── main.go             # Wiring only: connects infra → service → handler → HTTP
│   │   ├── Dockerfile
│   │   └── static/index.html   # Embedded web dashboard
│   └── inventory-service/
│       ├── main.go             # Wiring only: connects infra → service → handler
│       └── Dockerfile
│
├── internal/                   # All business logic — nothing in cmd/ except wiring
│   ├── order/                  # Order service domain
│   │   ├── handler.go          # HTTP handlers (depends on OrderService interface)
│   │   ├── service.go          # OrderService interface + implementation
│   │   └── handler_test.go
│   │
│   ├── inventory/              # Inventory service domain
│   │   ├── repository.go       # StockRepository interface (data contract)
│   │   ├── service.go          # StockService interface + implementation
│   │   └── handler.go          # GRPCHandler, KafkaHandler, StartGRPC()
│   │
│   ├── models/
│   │   └── order.go            # Order, CreateOrderRequest, OrderEvent, Stats
│   │
│   ├── config/
│   │   └── config.go           # Env-var config with localhost defaults
│   │
│   └── infrastructure/         # Technical implementations (no business logic)
│       ├── store/              # Redis — implements inventory.StockRepository
│       │   ├── client.go       # Connection + ping
│       │   └── stock.go        # Stock ops, idempotency, analytics, Lua script
│       ├── inventoryrpc/
│       │   └── client.go       # gRPC client used by order-service
│       ├── kafka/
│       │   ├── producer.go     # Sarama SyncProducer wrapper
│       │   └── consumer.go     # Sarama ConsumerGroup wrapper
│       └── pb/                 # Generated protobuf code (do not edit)
│           ├── inventory.pb.go
│           └── inventory_grpc.pb.go
│
├── proto/inventory.proto        # Source of truth for gRPC interface
├── deployments/
│   └── docker-compose.yml      # Full stack: Kafka + Redis + Kafka UI + both services
├── Makefile                    # Common commands
└── plan.md                     # Improvement task log
```

### Clean Architecture

Each service follows the **Handler → Service → Repository** pattern:

```
HTTP/gRPC/Kafka                    Business Logic              Data Access
──────────────                     ──────────────              ───────────
order/handler.go   ──calls──►  order/service.go   ──uses──►  infrastructure/store/stock.go
                               (OrderService)                  via StockReader
                               (interface)                     interface

inventory/handler.go ──calls──►  inventory/service.go  ──uses──►  infrastructure/store/stock.go
(GRPCHandler,                   (StockService)                    via StockRepository
 KafkaHandler)                  (interface)                       interface
```

Dependency direction: **always inward**. Handlers know about services; services know about repository interfaces; nothing in the inner layers knows about Kafka, HTTP, or Redis concretely.

## Getting Started

### Prerequisites

- Go 1.26+
- Docker & Docker Compose

### Quick Start (Everything in Docker)

```bash
make up          # start Kafka + Redis + Kafka UI + both services
make order       # send a test order
```

### Run Services Locally

```bash
make up          # start infrastructure only (Kafka, Redis, Kafka UI)

# Terminal 1
make run-inventory

# Terminal 2
make run-order
```

### Makefile Targets

```
make up            Start Docker infrastructure
make down          Stop Docker infrastructure
make run-inventory Run inventory service locally
make run-order     Run order service locally
make build         Compile binaries to bin/
make test          Run all tests
make proto         Regenerate protobuf code
make clean         Remove compiled binaries
make order         Send a test POST /orders (requires running services)
```

## API

### POST /orders — Create an order

Checks stock synchronously via gRPC, then publishes `order.created` to Kafka.

```bash
curl -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"item":"laptop","quantity":2}'
```

**201 Created:**
```json
{
  "id": "62db392a-f1d3-45be-8549-3c7ebfdb8d60",
  "item": "laptop",
  "quantity": 2,
  "status": "created",
  "created_at": "2026-05-27T18:02:48.075Z"
}
```

**409 Conflict** (insufficient stock):
```json
{ "error": "insufficient stock (need 5, have 2)", "current_stock": 2 }
```

### GET /api/stocks — Current inventory

```json
{ "laptop": 8, "mouse": 45, "keyboard": 30, "monitor": 15, "headset": 25 }
```

### GET /api/stats — Analytics counters

```json
{
  "total_orders": 3,
  "failed_orders": 0,
  "items_sold": { "laptop": 2 }
}
```

Items are discovered dynamically — no hardcoded list.

### GET /health

```json
{ "status": "ok", "service": "order-service" }
```

### Observe

| URL | Purpose |
|-----|---------|
| http://localhost:8080/static/ | Web dashboard — create orders, watch stock and analytics |
| http://localhost:8090 | Kafka UI — inspect topics, messages, consumer groups |
| `docker logs -f inventory-service` | Watch order processing in real time |

## Key Concepts Demonstrated

| Concept | Where |
|---------|-------|
| **Clean architecture** | `internal/order/` and `internal/inventory/` — Handler → Service → Repository with interfaces |
| **Dependency inversion** | `StockRepository`, `StockService`, `OrderService` are interfaces; concrete types injected at wiring time |
| **Hybrid sync + async** | gRPC for the pre-order stock check (sync reject); Kafka for the actual deduction (async, scalable) |
| **Atomic stock deduction** | Lua script in `store/stock.go` — check + deduct in one Redis operation, eliminating TOCTOU race |
| **Idempotency** | `processed:{orderID}` key with 24h TTL — Kafka redeliveries are safe |
| **Non-blocking Redis scan** | `SCAN` cursor loop instead of `KEYS *` in `GetAllStock` and `GetAllItemStats` |
| **Consumer groups** | Kafka splits partitions across group members; multiple inventory-service instances share the work |
| **Event-driven analytics** | Every `order.created` event increments `stats:items:{item}` and `stats:total_orders` in Redis |
| **Protobuf / gRPC** | `proto/inventory.proto` defines the contract; generated code in `internal/pb/` |
| **Embedded static files** | Go `//go:embed` serves the dashboard from the binary — no file path dependency |
| **KRaft mode Kafka** | Modern Kafka without Zookeeper |

## Environment Variables

| Variable | Default | Used by |
|----------|---------|---------|
| `KAFKA_BROKERS` | `localhost:9092` | both |
| `KAFKA_TOPIC` | `orders` | both |
| `REDIS_ADDR` | `localhost:6379` | both |
| `INVENTORY_GRPC_ADDR` | `localhost:50051` | order-service |
| `GRPC_PORT` | `50051` | inventory-service |
| `HTTP_PORT` | `8080` | order-service |
| `KAFKA_GROUP_ID` | `inventory-service` | inventory-service |

Inside Docker containers use `kafka:19092` and `redis:6379`.

## Learning Roadmap

- [x] Project setup and Docker basics
- [x] Kafka fundamentals — topics, partitions, producers, consumers, consumer groups
- [x] Order Service — HTTP API publishes events to Kafka
- [x] Inventory Service — consumer group processes order events, manages stock in Redis
- [x] Redis — stock management, idempotency, analytics counters
- [x] Full system in Docker — all services containerized with docker-compose
- [x] Web dashboard — real-time UI for testing and observing the system
- [x] gRPC — synchronous stock check before accepting orders; Protobuf contract
- [x] Clean architecture — Handler → Service → Repository with dependency inversion
- [x] Atomic operations — Lua script eliminates TOCTOU race in stock deduction
- [x] Makefile — all common commands in one place

## Areas to Improve

### Reliability

- [ ] **Dead Letter Queue (DLQ)** — move failed messages to `orders.dlq` after N retries instead of dropping
- [ ] **Retry with backoff** — exponential backoff for transient Kafka/Redis failures
- [ ] **Circuit breaker** — prevent cascading failures when a dependency is temporarily down
- [ ] **Graceful shutdown for Order Service** — drain in-flight HTTP requests before stopping

### Data & Consistency

- [ ] **Persistent database** — add PostgreSQL for durable order storage; Redis is cache, not source of truth
- [ ] **Outbox pattern** — write order to DB and publish to Kafka atomically to prevent data loss on crash
- [ ] **Schema validation** — validate Kafka message schema (JSON Schema or Protobuf) to catch bad data early

### Scalability

- [ ] **Multiple consumer instances** — run 2+ inventory-service containers; Kafka rebalances partitions automatically
- [ ] **Kafka partitioning strategy** — test with more partitions and observe rebalancing behaviour
- [ ] **Async Kafka producer** — switch from SyncProducer to AsyncProducer for higher throughput
- [ ] **gRPC streaming** — server-side streaming for real-time stock updates instead of polling

### Observability

- [ ] **Structured logging** — replace `log.Printf` with `slog` or `zap` for JSON log output
- [ ] **Metrics** — expose Prometheus metrics (request latency, Kafka lag, Redis operations)
- [ ] **Distributed tracing** — OpenTelemetry to trace an order from HTTP → Kafka → Redis
- [ ] **Consumer lag monitoring** — alert when inventory-service falls behind processing

### Security

- [ ] **Authentication** — add API key or JWT to `POST /orders`
- [ ] **Rate limiting** — protect the order creation endpoint from abuse
- [ ] **TLS** — encrypt Kafka and Redis connections in production
- [ ] **Kafka ACLs** — restrict which services can produce/consume on which topics

### Features

- [ ] **Notification Service** — second consumer group that sends order confirmations
- [ ] **Order cancellation** — publish `order.cancelled` events and restore stock
- [ ] **Order status tracking** — `GET /orders/:id` for clients to query status
- [ ] **Stock replenishment** — admin endpoint `POST /api/stocks/:item` to add inventory
- [ ] **WebSocket** — push real-time updates to the dashboard instead of polling
