## Summary

<!-- What does this PR change and why? Keep it short. -->

## Affected area(s)

<!-- Tick all that apply. -->

- [ ] Order Service (`cmd/order-service`, `internal/order`)
- [ ] Inventory Service (`cmd/inventory-service`, `internal/inventory`)
- [ ] Infrastructure (`internal/infrastructure/{store,kafka,inventoryrpc,pb}`)
- [ ] Shared models / config (`internal/models`, `internal/config`)
- [ ] Proto / gRPC contract (`proto/inventory.proto`)
- [ ] Deployments / CI (`deployments/`, `.github/`, `Makefile`)
- [ ] Docs

## Type of change

- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Refactor / cleanup
- [ ] Docs / chore

## How to test

<!-- Commands a reviewer can run to verify. Example below. -->

```bash
make up
make run-inventory   # terminal 1
make run-order       # terminal 2
make order           # end-to-end POST /orders
```

## Checklist

- [ ] `go build ./...` passes
- [ ] `go vet ./...` passes
- [ ] `make test` (unit tests with `-race`) passes
- [ ] Integration tests pass if store/Redis touched (`make test-integration`)
- [ ] Regenerated protobuf with `make proto` if `proto/inventory.proto` changed
- [ ] Did not edit generated code in `internal/infrastructure/pb/` by hand
- [ ] Updated `CLAUDE.md` / docs if architecture, env vars, or commands changed
- [ ] No secrets or local config committed

## Notes for reviewers

<!-- Anything tricky: TOCTOU/idempotency concerns, Kafka ordering, schema changes, migration steps. -->
