package inventory

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/niphitphon8bit/kafka-order-lab/internal/models"
)

// StockService is the business logic contract for stock management.
// Both the gRPC handler and the Kafka handler depend on this interface.
type StockService interface {
	InitStock(ctx context.Context, items map[string]int) error
	CheckStock(ctx context.Context, item string, quantity int) (available bool, current int32, message string, err error)
	GetAllStock(ctx context.Context) (map[string]int, error)
	ProcessOrder(ctx context.Context, order models.Order) error
}

type stockService struct {
	repo StockRepository
}

func NewStockService(repo StockRepository) StockService {
	return &stockService{repo: repo}
}

func (s *stockService) InitStock(ctx context.Context, items map[string]int) error {
	return s.repo.InitStock(ctx, items)
}

// CheckStock returns whether sufficient stock exists without modifying it.
// Used by the gRPC server to answer pre-order stock queries from order-service.
func (s *stockService) CheckStock(ctx context.Context, item string, quantity int) (bool, int32, string, error) {
	current, err := s.repo.GetStock(ctx, item)
	if err != nil {
		return false, 0, "", fmt.Errorf("failed to get stock for %s: %w", item, err)
	}

	if current == -1 {
		return false, 0, fmt.Sprintf("item '%s' not found in inventory", item), nil
	}

	if current >= quantity {
		return true, int32(current), fmt.Sprintf("%d in stock", current), nil
	}
	return false, int32(current), fmt.Sprintf("insufficient stock (need %d, have %d)", quantity, current), nil
}

func (s *stockService) GetAllStock(ctx context.Context) (map[string]int, error) {
	return s.repo.GetAllStock(ctx)
}

// ProcessOrder handles an order.created event:
// idempotency check → atomic stock deduction → analytics update.
// Returning nil commits the Kafka offset; the error path logs but does not retry
// (failed orders are counted and skipped to avoid poison-pill loops).
func (s *stockService) ProcessOrder(ctx context.Context, order models.Order) error {
	processed, err := s.repo.IsProcessed(ctx, order.ID)
	if err != nil {
		return fmt.Errorf("idempotency check failed: %w", err)
	}
	if processed {
		log.Printf("⏭️  Order %s already processed, skipping", order.ID)
		return nil
	}

	newQty, err := s.repo.CheckAndDeductStock(ctx, order.Item, order.Quantity)
	if err != nil {
		log.Printf("❌ Order %s: %v", order.ID, err)
		s.repo.IncrementCounter(ctx, "failed_orders", 1)
		return nil
	}

	if err := s.repo.MarkProcessed(ctx, order.ID, 24*time.Hour); err != nil {
		log.Printf("Warning: failed to mark order %s as processed: %v", order.ID, err)
	}

	s.repo.IncrementCounter(ctx, "total_orders", 1)
	s.repo.IncrementCounter(ctx, fmt.Sprintf("items:%s", order.Item), int64(order.Quantity))

	log.Printf("✅ Order %s: reserved %d x '%s' (stock remaining: %d)",
		order.ID, order.Quantity, order.Item, newQty)

	return nil
}
