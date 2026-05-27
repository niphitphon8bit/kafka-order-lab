# ---- Stage 1: Build ----
FROM golang:1.26-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o inventory-service ./cmd/inventory-service

# ---- Stage 2: Run ----
FROM alpine:3.20

WORKDIR /app

COPY --from=builder /app/inventory-service .

CMD ["./inventory-service"]
