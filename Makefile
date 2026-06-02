.PHONY: up down run-inventory run-order build test proto clean order

## Infrastructure
up:
	docker compose -f deployments/docker-compose.yml up -d

down:
	docker compose -f deployments/docker-compose.yml down

## Run services locally
run-inventory:
	go run ./cmd/inventory-service/

run-order:
	go run ./cmd/order-service/

## Build binaries into bin/
build:
	go build -o bin/inventory-service ./cmd/inventory-service/
	go build -o bin/order-service ./cmd/order-service/

## Tests
test:
	go test ./...

## Integration tests — requires Redis on localhost:6379 (uses DB 15, safe to wipe)
test-integration:
	go test -tags integration ./internal/infrastructure/store/...

## Regenerate protobuf Go code after editing proto/inventory.proto
## Output location is controlled by go_package in proto/inventory.proto
## → internal/infrastructure/pb/
proto:
	protoc --go_out=. --go-grpc_out=. proto/inventory.proto

## Remove compiled binaries
clean:
	rm -f bin/inventory-service bin/order-service

## Send a test order (services must be running)
order:
	curl -s -X POST http://localhost:8080/orders \
	  -H "Content-Type: application/json" \
	  -d '{"item":"laptop","quantity":1,"user_id":"user-123"}' | jq .
