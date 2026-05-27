# kafka-order-lab

A hands-on learning project for building event-driven microservices with **Go**, **Kafka**, **Redis**, and **Docker**.

## Architecture

```
Client → Order Service → Kafka → Inventory Service  → Redis (stock cache)
                               → Notification Service
                               → Analytics Service   → Redis (counters)
```

## Tech Stack

- **Go** — microservices
- **Apache Kafka** — message broker for async, event-driven communication
- **Redis** — in-memory caching and data structures
- **Docker** — containerized deployment

## Services

| Service | Description |
|---------|-------------|
| Order Service | Accepts orders via HTTP, publishes events to Kafka |
| Inventory Service | Consumes order events, manages stock via Redis |
| Notification Service | Consumes order events, sends notifications |

## Getting Started

```bash
# Start infrastructure (Kafka, Redis)
docker-compose -f deployments/docker-compose.yml up -d

# Run a service
go run cmd/order-service/main.go
```

## Learning Roadmap

- [x] Project setup
- [ ] Docker basics
- [ ] Kafka fundamentals
- [ ] Order Service (producer)
- [ ] Inventory Service (consumer)
- [ ] Redis caching
- [ ] Full system in Docker
