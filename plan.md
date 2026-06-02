# Codebase Improvement Plan

Fixes to make the project easier to read, navigate, and understand.

---

## Tasks

| # | Task | Status |
|---|------|--------|
| 1 | Remove compiled binaries from git (`.gitignore` already correct, binaries not tracked) | ✅ Done |
| 2 | Rename `internal/grpc` → `internal/inventoryrpc` to eliminate `appgrpc` alias everywhere | ✅ Done |
| 3 | Rename `internal/redis` → `internal/store` to eliminate `appredis` alias everywhere | ✅ Done |
| 4 | Split `cmd/order-service/main.go` — move HTTP handlers to `handlers.go` | ✅ Done |
| 5 | Split `cmd/inventory-service/main.go` — move Kafka event processor to `processor.go` | ✅ Done |
| 6 | Fix `handleGetStats` to read item stats dynamically (stop hardcoding 5 item names) | ✅ Done |
| 7 | Fix `GetAllStock` — replace `KEYS` command with `SCAN` (non-blocking) | ✅ Done |
| 8 | Fix `CheckAndDeductStock` — make it atomic with a Lua script (eliminate TOCTOU race) | ✅ Done |
| 9 | Add `Makefile` at project root with all common commands | ✅ Done |
| 10 | Add tests for `internal/config` and HTTP handlers | ✅ Done |
| 11 | Refactor to clean architecture: handler → service → repository per service | ✅ Done |
| 12 | Move infrastructure packages under `internal/infrastructure/` for consistency | ✅ Done |
| 13 | Write `internal/order/service_test.go` — unit tests for OrderService | ✅ Done |
| 14 | Write `internal/inventory/service_test.go` — unit tests for StockService | ✅ Done |
| 15 | Write `internal/inventory/handler_test.go` — unit tests for KafkaHandler + GRPCHandler | ✅ Done |
| 16 | Write `internal/infrastructure/store/stock_test.go` — integration tests (Redis, build-tagged) | ✅ Done |

---

## Detail

### Task 2 — Rename `internal/grpc` → `internal/inventoryrpc`

**Problem:** The package is named `grpc`, which clashes with `google.golang.org/grpc`. Every importer
must alias it as `appgrpc`, which is opaque to a newcomer.

**Changes:**
- `mv internal/grpc internal/inventoryrpc`
- Change `package grpc` → `package inventoryrpc` in both files
- Update import path + remove alias in `cmd/order-service/main.go`
- Update import path + remove alias in `cmd/inventory-service/main.go`

---

### Task 3 — Rename `internal/redis` → `internal/store`

**Problem:** Same clash — the package is named `redis`, colliding with `github.com/redis/go-redis/v9`.
Imports require `appredis` alias everywhere.

**Changes:**
- `mv internal/redis internal/store`
- Change `package redis` → `package store` in both files
- Update import path + remove alias in both `main.go` files
- Update import in `internal/grpc/server.go` (now `internal/inventoryrpc/server.go`)

---

### Task 4 — Split order-service `main.go`

**Problem:** `main.go` contains all HTTP handler functions. Newcomers expect `main.go` to be just
wiring/startup, not business logic.

**Changes:**
- Create `cmd/order-service/handlers.go` with an `OrderHandlers` struct
- Move `handleHealth`, `handleCreateOrder`, `handleGetStocks`, `handleGetStats` to the struct
- Remove global `var producer`, `redisClient`, `inventoryClient` from `main.go`
- `main.go` constructs `OrderHandlers` and registers routes

---

### Task 5 — Split inventory-service `main.go`

**Problem:** `main.go` contains Kafka event-handling logic (`handleOrderEvent`, `processOrderCreated`).

**Changes:**
- Create `cmd/inventory-service/processor.go` with an `OrderProcessor` struct
- Move `handleOrderEvent` and `processOrderCreated` into the struct
- Remove global `var redisClient` from `main.go`
- `main.go` constructs `OrderProcessor` and passes its method to the Kafka consumer

---

### Task 6 — Dynamic stats (remove hardcoded item list)

**Problem:** `handleGetStats` hardcodes laptop, mouse, keyboard, monitor, headset. Adding a new item to
inventory requires editing order-service too (a separate service).

**Changes:**
- Add `GetAllItemStats(ctx) (map[string]int64, error)` to `internal/store`
  using `SCAN stats:items:*` to discover items dynamically
- Replace the 5 hardcoded `GetCounter` calls in `handleGetStats`

---

### Task 7 — Replace `KEYS` with `SCAN` in `GetAllStock`

**Problem:** `KEYS stock:*` blocks the entire Redis server during the scan.

**Change:** Use cursor-based `SCAN` loop in `GetAllStock`.

---

### Task 8 — Atomic `CheckAndDeductStock` with Lua

**Problem:** `CheckAndDeductStock` does `GetStock` (read) then `DeductStock` (write) in two
separate Redis round-trips. Two concurrent Kafka messages for the same low-stock item can both
pass the check before either deducts — causing negative stock (oversell).

**Change:** Replace with a single Lua script executed via `EVAL` / `redis.Script`. Lua scripts
in Redis are executed atomically.

---

### Task 9 — Makefile

**Problem:** All commands live only in `CLAUDE.md`. A `Makefile` makes them discoverable and
runnable with tab-completion.

**Targets:** `up`, `down`, `run-inventory`, `run-order`, `build`, `test`, `proto`, `clean`, `order`

---

### Task 10 — Tests

**Problem:** Zero test files — behavior is only discoverable by reading source code.

**Files added:**
- `internal/config/config_test.go` — env var loading and defaults
- `internal/order/handler_test.go` — HTTP handler behavior (using `httptest`)

---

### Task 12 — Move to `internal/infrastructure/`

**Problem:** `store/`, `inventoryrpc/`, `kafka/`, `pb/` sat alongside domain packages at the top of
`internal/`, making the naming inconsistent with `order/` and `inventory/`.

**Changes:**
- Move `internal/store/` → `internal/infrastructure/store/`
- Move `internal/inventoryrpc/` → `internal/infrastructure/inventoryrpc/`
- Move `internal/kafka/` → `internal/infrastructure/kafka/`
- Move `internal/pb/` → `internal/infrastructure/pb/`
- Update all import paths in `cmd/`, `internal/inventory/handler.go`, and `infrastructure/inventoryrpc/client.go`
- Update `proto/inventory.proto` `go_package` option
- Update `Makefile` proto target

---

### Task 13 — `internal/order/service_test.go`

**What to test:**
- `CreateOrder` happy path → returns Order with correct fields, publishes event
- `CreateOrder` when gRPC says insufficient stock → returns `InsufficientStockError`
- `CreateOrder` when gRPC call fails → proceeds anyway (fallback), still publishes
- `CreateOrder` when Kafka publish fails → returns error
- `GetStats` happy path → assembles Stats from StockReader
- `GetStats` when `GetAllItemStats` fails → returns error

**Approach:** stub all three interfaces (`StockReader`, `InventoryChecker`, `EventPublisher`).

---

### Task 14 — `internal/inventory/service_test.go`

**What to test:**
- `ProcessOrder` happy path → deducts stock, marks processed, increments counters
- `ProcessOrder` already processed (idempotency) → skips silently, returns nil
- `ProcessOrder` insufficient stock → increments `failed_orders`, returns nil (not error)
- `ProcessOrder` idempotency check fails → returns error (only case that retries)
- `CheckStock` sufficient stock → returns available=true
- `CheckStock` insufficient stock → returns available=false with message
- `CheckStock` item not found (GetStock returns -1) → returns available=false with "not found" message
- `CheckStock` repo error → returns error

**Approach:** stub `StockRepository` interface.

---

### Task 15 — `internal/inventory/handler_test.go`

**What to test:**
- `KafkaHandler.Handle` with valid `order.created` JSON → calls `ProcessOrder`
- `KafkaHandler.Handle` with invalid JSON → returns error
- `KafkaHandler.Handle` with unknown event type → returns nil (skips)
- `GRPCHandler.CheckStock` → delegates to service, maps response fields correctly
- `GRPCHandler.GetStock` → delegates to service, maps stock map to proto items

**Approach:** stub `StockService` interface.

---

### Task 16 — `internal/infrastructure/store/stock_test.go`

**What to test (requires live Redis — `//go:build integration` tag):**
- `CheckAndDeductStock` sufficient stock → deducts, returns new quantity
- `CheckAndDeductStock` insufficient stock → returns error, stock unchanged
- `CheckAndDeductStock` item not found → returns error
- `InitStock` second call does not overwrite existing values (HSETNX behaviour)
- `IsProcessed` / `MarkProcessed` round-trip with TTL
- `GetAllStock` returns all seeded items via SCAN

**Approach:** connect to `localhost:6379`, flush test keys before/after each test.
