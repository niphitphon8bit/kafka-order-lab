# ---- Stage 1: Build ----
# Start from the official Go image to compile our code
FROM golang:1.26-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy go.mod first (dependency caching layer)
# Docker caches each layer. If go.mod hasn't changed,
# Docker skips re-downloading dependencies on next build.
COPY go.mod ./

# Download dependencies (none yet, but good practice)
RUN go mod download

# Copy the rest of the source code
COPY . .

# Build the binary
# CGO_ENABLED=0 = static binary (no C dependencies)
# -o order-service = output binary name
RUN CGO_ENABLED=0 GOOS=linux go build -o order-service ./cmd/order-service

# ---- Stage 2: Run ----
# Use a minimal image for the final container (only ~5MB!)
FROM alpine:3.20

WORKDIR /app

# Copy ONLY the binary from the build stage
COPY --from=builder /app/order-service .

# Tell Docker this container listens on port 8080
EXPOSE 8080

# Command to run when the container starts
CMD ["./order-service"]
