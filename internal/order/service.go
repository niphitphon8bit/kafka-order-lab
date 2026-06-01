package order

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/niphitphon8bit/kafka-order-lab/internal/models"
)

// Narrow dependency interfaces — OrderService only sees what it actually needs.
// Any concrete type that has these methods satisfies the interface (duck typing).

// StockReader is for reading stock and analytics data from the store.
// Satisfied by *store.Client.
type StockReader interface {
	GetAllStock(ctx context.Context) (map[string]int, error)
	GetAllItemStats(ctx context.Context) (map[string]int64, error)
	GetCounter(ctx context.Context, name string) (int64, error)
}

// InventoryChecker is for the synchronous pre-order stock check via gRPC.
// Satisfied by *inventoryrpc.InventoryClient.
type InventoryChecker interface {
	CheckStock(ctx context.Context, item string, quantity int) (bool, int, string, error)
}

// EventPublisher is for publishing order events to Kafka.
// Satisfied by *kafka.Producer.
type EventPublisher interface {
	SendMessage(key string, value any) (int32, int64, error)
}

// OrderService is the business logic contract for the order service.
type OrderService interface {
	CreateOrder(ctx context.Context, req models.CreateOrderRequest) (models.Order, error)
	GetStocks(ctx context.Context) (map[string]int, error)
	GetStats(ctx context.Context) (models.Stats, error)
}

type orderService struct {
	stocks    StockReader
	inventory InventoryChecker
	publisher EventPublisher
}

func NewOrderService(stocks StockReader, inv InventoryChecker, pub EventPublisher) OrderService {
	return &orderService{
		stocks:    stocks,
		inventory: inv,
		publisher: pub,
	}
}

// CreateOrder checks stock via gRPC, then publishes an order.created event to Kafka.
// If the gRPC check fails (inventory service down), the order proceeds anyway so that
// we don't drop orders when inventory is temporarily unreachable.
func (s *orderService) CreateOrder(ctx context.Context, req models.CreateOrderRequest) (models.Order, error) {
	available, currentStock, msg, err := s.inventory.CheckStock(ctx, req.Item, req.Quantity)
	if err != nil {
		// Inventory service unreachable — log and proceed (let Kafka consumer handle it)
		log.Printf("Warning: gRPC stock check failed: %v (proceeding anyway)", err)
	} else if !available {
		return models.Order{}, &InsufficientStockError{
			Message:      msg,
			CurrentStock: currentStock,
		}
	}

	order := models.Order{
		ID:        uuid.New().String(),
		Item:      req.Item,
		Quantity:  req.Quantity,
		Status:    "created",
		CreatedAt: time.Now(),
	}

	event := models.OrderEvent{
		EventType: "order.created",
		Order:     order,
	}

	if _, _, err := s.publisher.SendMessage(order.ID, event); err != nil {
		return models.Order{}, fmt.Errorf("failed to publish order event: %w", err)
	}

	return order, nil
}

func (s *orderService) GetStocks(ctx context.Context) (map[string]int, error) {
	return s.stocks.GetAllStock(ctx)
}

func (s *orderService) GetStats(ctx context.Context) (models.Stats, error) {
	totalOrders, _ := s.stocks.GetCounter(ctx, "total_orders")
	failedOrders, _ := s.stocks.GetCounter(ctx, "failed_orders")
	itemsSold, err := s.stocks.GetAllItemStats(ctx)
	if err != nil {
		return models.Stats{}, fmt.Errorf("failed to get item stats: %w", err)
	}

	return models.Stats{
		TotalOrders:  totalOrders,
		FailedOrders: failedOrders,
		ItemsSold:    itemsSold,
	}, nil
}

// InsufficientStockError is returned by CreateOrder when gRPC confirms stock is too low.
// The HTTP handler uses this type to distinguish a 409 from a 500.
type InsufficientStockError struct {
	Message      string
	CurrentStock int
}

func (e *InsufficientStockError) Error() string {
	return e.Message
}
