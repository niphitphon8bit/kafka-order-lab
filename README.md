# kafka-order-lab

A hands-on learning project for building event-driven microservices with **Go**, **Kafka**, **Redis**, and **Docker**.

## Architecture

```
                         ┌─────────────────┐
  POST /orders ─────────►│  Order Service   │
  (HTTP client)          │  (Producer)      │
                         └────────┬─────────┘
                                  │
                           publish to Kafka
                                  │
                                  ▼
                         ┌─────────────────┐
                         │     Kafka        │
                         │  topic: "orders" │
                         └────────┬─────────┘
                                  │
                             consume
                                  │
                                  ▼
                         ┌─────────────────┐       ┌───────────┐
                         │Inventory Service │──────►│   Redis    │
                         │  (Consumer)      │       │           │
                         └─────────────────┘       │ - Stock   │
                                                   │ - Idempot.│
                                                   │ - Counters│
                                                   └───────────┘
```

## Tech Stack

- **Go 1.26** — microservices
- **Apache Kafka 3.9** (KRaft mode) — message broker for async, event-driven communication
- **Redis 7** — in-memory caching, stock management, idempotency, analytics
- **Docker & Docker Compose** — containerized deployment

## Services

| Service | Role | Description |
|---------|------|-------------|
| **Order Service** | Producer | Accepts orders via `POST /orders`, publishes `order.created` events to Kafka |
| **Inventory Service** | Consumer | Consumes order events, checks/deducts stock in Redis, tracks analytics |

## Project Structure

```
kafka-order-lab/
├── cmd/
│   ├── order-service/          # HTTP API → Kafka producer
│   │   ├── main.go
│   │   └── Dockerfile
│   └── inventory-service/      # Kafka consumer → Redis
│       └── main.go
├── internal/
│   ├── kafka/
│   │   ├── producer.go         # Kafka sync producer wrapper
│   │   └── consumer.go         # Kafka consumer group wrapper
│   ├── redis/
│   │   ├── client.go           # Redis connection
│   │   └── stock.go            # Stock, idempotency, analytics
│   └── models/
│       └── order.go            # Order & OrderEvent structs
├── deployments/
│   └── docker-compose.yml      # Kafka + Kafka UI + Redis
├── go.mod
└── README.md
```

## Getting Started

### Prerequisites

- Go 1.26+
- Docker & Docker Compose

### 1. Start Infrastructure

```bash
# Start Kafka, Kafka UI, and Redis
docker compose -f deployments/docker-compose.yml up -d

# Verify all containers are running
docker ps
```

### 2. Create Kafka Topic

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 2 --replication-factor 1
```

### 3. Run Services (3 terminals)

**Terminal 1 — Inventory Service (Consumer):**
```bash
go run ./cmd/inventory-service/
```

**Terminal 2 — Order Service (Producer):**
```bash
go run ./cmd/order-service/
```

**Terminal 3 — Send Orders (Client):**
```bash
# Create an order
curl -s -X POST http://localhost:8080/orders \
  -H "Content-Type: application/json" \
  -d '{"item":"laptop","quantity":2}'

# Check health
curl -s http://localhost:8080/health
```

### 4. Observe

- **Terminal 1** — see inventory logs (stock deductions, idempotency skips)
- **Kafka UI** — open http://localhost:8090 to inspect topics and messages
- **Redis CLI** — check stock and counters:
  ```bash
  docker exec redis redis-cli HGET stock:laptop quantity
  docker exec redis redis-cli GET stats:total_orders
  ```

## API

### POST /orders

Create a new order.

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
| Idempotency | Redis tracks processed order IDs to prevent duplicates |
| Stock management | Redis HASH with atomic HINCRBY for thread-safe deductions |
| Analytics counters | Redis INCR for real-time order/item tracking |
| Multi-stage Docker build | Tiny production images (~17MB) |
| KRaft mode | Modern Kafka without Zookeeper dependency |

## Learning Roadmap

- [x] Project setup
- [x] Docker basics — Dockerfile, multi-stage builds, docker-compose
- [x] Kafka fundamentals — topics, partitions, producers, consumers
- [x] Order Service (producer) — HTTP API publishes events to Kafka
- [x] Inventory Service (consumer) — consumer group processes order events
- [x] Redis caching — stock management, idempotency, analytics counters
- [ ] Full system in Docker — all services containerized with docker-compose
