package inventory

import (
	"context"
	"time"
)

// StockRepository is the data access contract for stock management.
// internal/store.*Client satisfies this interface via Go duck typing — no explicit declaration needed.
//
// This interface lets the service layer be tested with a mock repository
// without needing a real Redis connection.
type StockRepository interface {
	InitStock(ctx context.Context, items map[string]int) error
	GetStock(ctx context.Context, item string) (int, error)
	GetAllStock(ctx context.Context) (map[string]int, error)
	CheckAndDeductStock(ctx context.Context, item string, quantity int) (int, error)
	GetAllItemStats(ctx context.Context) (map[string]int64, error)
	GetCounter(ctx context.Context, name string) (int64, error)
	IncrementCounter(ctx context.Context, name string, by int64) (int64, error)
	IsProcessed(ctx context.Context, orderID string) (bool, error)
	MarkProcessed(ctx context.Context, orderID string, ttl time.Duration) error
}
