# kafka-order-lab

A hands-on learning project for building event-driven microservices with **Go**, **Kafka**, **Redis**, and **Docker**.

## Architecture

```
                         ┌─────────────────┐
  POST /orders ─────────►│  Order Service   │
  (HTTP client)          │  (Producer)      │──────────┐
                         └────────┬─────────┘          │
                                  │                    │ read-only
                           publish to Kafka            │
                                  │                    ▼
                                  ▼              ┌───────────┐
                         ┌─────────────────┐     │   Redis    │
                         │     Kafka        │     │           │
                         │  topic: "orders" │     │ - Stock   │
                         └────────┬─────────┘     │ - Idempot.│
                                  │               │ - Counters│
                             consume              └───────────┘
                                  │                    ▲
                                  ▼                    │ read/write
                         ┌─────────────────┐           │
                         │Inventory Service │───────────┘
                         │  (Consumer)      │
                         └─────────────────┘
```

## Tech Stack

- **Go 1.26** — microservices
- **Apache Kafka 3.9** (KRaft mode) — message broker for async, event-driven communication
- **Redis 7** — in-memory caching, stock management, idempotency, analytics
- **Docker & Docker Compose** — containerized deployment

## Services

| Service | Role | Description |
|---------|------|-------------|
| **Order Service** | Producer | HTTP API (`POST /orders`), publishes events to Kafka, serves web dashboard |
| **Inventory Service** | Consumer | Consumes order events, checks/deducts stock in Redis, tracks analytics |
| **Kafka UI** | Dashboard | Web UI for inspecting Kafka topics, messages, and consumer groups |

## Project Structure

```
kafka-order-lab/
├── cmd/
│   ├── order-service/
│   │   ├── main.go             # HTTP API + Kafka producer + dashboard
│   │   ├── Dockerfile
│   │   └── static/
│   │       └── index.html      # Web dashboard
│   └── inventory-service/
│       ├── main.go             # Kafka consumer + Redis processing
│       └── Dockerfile
├── internal/
│   ├── config/
│   │   └── config.go           # Environment-based configuration
│   ├── kafka/
│   │   ├── producer.go         # Kafka sync producer wrapper
│   │   └── consumer.go         # Kafka consumer group wrapper
│   ├── redis/
│   │   ├── client.go           # Redis connection
│   │   └── stock.go            # Stock, idempotency, analytics
│   └── models/
│       └── order.go            # Order & OrderEvent structs
├── deployments/
│   └── docker-compose.yml      # Full system: Kafka + Redis + services
├── go.mod
└── README.md
```

## Getting Started

### Prerequisites

- Go 1.26+
- Docker & Docker Compose

### Option 1: Run Everything in Docker (Recommended)

```bash
# Start the full system with one command
docker compose -f deployments/docker-compose.yml up -d --build

# Verify all containers are running
docker ps
```

### Option 2: Run Services Locally

```bash
# Start infrastructure only
docker compose -f deployments/docker-compose.yml up -d kafka redis kafka-ui

# Terminal 1: Inventory Service
go run ./cmd/inventory-service/

# Terminal 2: Order Service
go run ./cmd/order-service/
```

### Create Orders

**Web dashboard:** http://localhost:8080/static/

**cURL:**
```bash
curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"item":"laptop","quantity":2}'
```

### Observe

| URL | What |
|-----|------|
| http://localhost:8080/static/ | Web dashboard — create orders, view stock & analytics |
| http://localhost:8090 | Kafka UI — inspect topics, messages, consumer groups |
| `docker logs -f inventory-service` | Watch order processing in real-time |

## API

### POST /orders

Create a new order. Publishes an `order.created` event to Kafka.

```json
// Request
{ "item": "laptop", "quantity": 2 }

// Response (201 Created)
{
  "id": "62db392a-f1d3-45be-8549-3c7ebfdb8d60",
  "item": "laptop",
  "quantity": 2,
  "status": "created",
  "created_at": "2026-05-27T18:02:48.075Z"
}
```

### GET /api/stocks

Returns current inventory levels from Redis.

```json
{ "laptop": 8, "mouse": 45, "keyboard": 30, "monitor": 15, "headset": 25 }
```

### GET /api/stats

Returns analytics counters from Redis.

```json
{
  "total_orders": 3,
  "failed_orders": 1,
  "items_sold": { "laptop": 5, "mouse": 10, "keyboard": 0, "monitor": 2, "headset": 0 }
}
```

### GET /health

```json
{ "status": "ok", "service": "order-service" }
```

## Key Concepts Demonstrated

| Concept | Implementation |
|---------|---------------|
| Event-driven architecture | Order Service publishes events, Inventory Service reacts |
| Async communication | Producer returns immediately, consumer processes independently |
| Consumer groups | Kafka splits partitions across group members for scaling |
| Idempotency | Redis tracks processed order IDs (TTL 24h) to prevent duplicates |
| Stock management | Redis HASH with atomic HINCRBY for thread-safe deductions |
| Analytics counters | Redis INCR for real-time order/item tracking |
| Environment config | Services use env vars — works locally and in Docker |
| Multi-stage Docker build | Tiny production images (~17MB) |
| KRaft mode | Modern Kafka without Zookeeper dependency |
| Embedded static files | Go `embed` serves dashboard from the binary |
| HTTP vs gRPC vs Kafka | HTTP for external clients, gRPC for internal sync, Kafka for async events (planned) |

## Learning Roadmap

- [x] Project setup
- [x] Docker basics — Dockerfile, multi-stage builds, docker-compose
- [x] Kafka fundamentals — topics, partitions, producers, consumers
- [x] Order Service (producer) — HTTP API publishes events to Kafka
- [x] Inventory Service (consumer) — consumer group processes order events
- [x] Redis caching — stock management, idempotency, analytics counters
- [x] Full system in Docker — all services containerized with docker-compose
- [x] Web dashboard — real-time UI for testing and observing the system

## Areas to Improve

### Reliability & Resilience

- [ ] **Dead Letter Queue (DLQ)** — move poison/failed messages to a separate `orders.dlq` topic after N retries instead of dropping them
- [ ] **Retry with backoff** — implement exponential backoff for transient failures (Kafka/Redis connection issues)
- [ ] **Circuit breaker** — prevent cascading failures when Redis or Kafka is temporarily unavailable
- [ ] **Graceful shutdown for Order Service** — drain in-flight HTTP requests and flush the Kafka producer before stopping

### Data & Consistency

- [ ] **Persistent database** — add PostgreSQL for permanent order storage (Redis is for cache/speed, not source of truth)
- [ ] **Outbox pattern** — write order to DB and publish to Kafka in a single transaction to prevent data loss
- [ ] **Race condition in stock check** — `CheckAndDeductStock` has a TOCTOU gap between check and deduct; use a Lua script or Redis transaction for true atomicity
- [ ] **Schema validation** — validate Kafka message schema (e.g., with JSON Schema or Protobuf) to catch bad data before it enters the pipeline

### Communication & Scalability

- [ ] **gRPC for internal calls** — replace HTTP between services with gRPC (Protocol Buffers) for fast, type-safe, binary communication; use for sync operations like stock checks before accepting orders
- [ ] **gRPC streaming** — use server-side streaming for real-time stock level updates instead of polling
- [ ] **API gateway pattern** — Order Service accepts HTTP from external clients, translates to gRPC for internal service calls
- [ ] **Multiple consumer instances** — run 2+ inventory-service containers to process partitions in parallel
- [ ] **Kafka partitioning strategy** — test with more partitions and observe how consumer groups rebalance
- [ ] **Async Kafka producer** — switch from SyncProducer to AsyncProducer for higher throughput
- [ ] **Redis connection pooling** — tune pool size for high concurrency

### Observability

- [ ] **Structured logging** — replace `log.Printf` with structured logger (e.g., `slog`, `zap`) for JSON log output
- [ ] **Consumer lag monitoring** — alert when inventory-service falls behind on processing
- [ ] **Metrics** — expose Prometheus metrics (request latency, Kafka produce/consume rates, Redis hit/miss)
- [ ] **Distributed tracing** — add OpenTelemetry to trace an order from HTTP request through Kafka to Redis
- [ ] **Health check for Inventory Service** — add an HTTP health endpoint so Docker can monitor it

### Security & Operations

- [ ] **Authentication** — add API key or JWT auth to `POST /orders`
- [ ] **Rate limiting** — prevent abuse of the order creation endpoint
- [ ] **Kafka ACLs** — restrict which services can produce/consume on which topics
- [ ] **Redis password** — add authentication for Redis in production
- [ ] **TLS everywhere** — encrypt Kafka and Redis connections
- [ ] **CI/CD pipeline** — automated testing, building, and deployment
- [ ] **Helm charts / Kubernetes** — deploy to a real orchestration platform

### Features

- [ ] **Notification Service** — add a second consumer group that sends order confirmation notifications
- [ ] **Order cancellation** — publish `order.cancelled` events and restore stock
- [ ] **Order status tracking** — let clients query order status (`GET /orders/:id`)
- [ ] **Stock replenishment** — admin endpoint to add stock (`POST /api/stocks/:item`)
- [ ] **WebSocket** — push real-time stock/order updates to the dashboard instead of polling
