package inventory

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/niphitphon8bit/kafka-order-lab/internal/models"
	"github.com/niphitphon8bit/kafka-order-lab/internal/infrastructure/pb"
)

// ---- gRPC Handler ----

// GRPCHandler implements pb.InventoryServiceServer.
// It translates gRPC requests into StockService calls — no Redis logic here.
type GRPCHandler struct {
	pb.UnimplementedInventoryServiceServer
	svc StockService
}

func NewGRPCHandler(svc StockService) *GRPCHandler {
	return &GRPCHandler{svc: svc}
}

func (h *GRPCHandler) CheckStock(ctx context.Context, req *pb.CheckStockRequest) (*pb.CheckStockResponse, error) {
	log.Printf("[gRPC] CheckStock: item=%s quantity=%d", req.Item, req.Quantity)

	available, current, msg, err := h.svc.CheckStock(ctx, req.Item, int(req.Quantity))
	if err != nil {
		return nil, fmt.Errorf("CheckStock failed: %w", err)
	}

	return &pb.CheckStockResponse{
		Available:    available,
		CurrentStock: current,
		Message:      msg,
	}, nil
}

func (h *GRPCHandler) GetStock(ctx context.Context, req *pb.GetStockRequest) (*pb.GetStockResponse, error) {
	log.Println("[gRPC] GetStock: fetching all items")

	stocks, err := h.svc.GetAllStock(ctx)
	if err != nil {
		return nil, fmt.Errorf("GetStock failed: %w", err)
	}

	var items []*pb.StockItem
	for item, qty := range stocks {
		items = append(items, &pb.StockItem{
			Item:     item,
			Quantity: int32(qty),
		})
	}

	return &pb.GetStockResponse{Items: items}, nil
}

// StartGRPC launches the gRPC server on the given port in a goroutine.
// The caller should defer server.GracefulStop().
func StartGRPC(port string, svc StockService) (*grpc.Server, error) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	server := grpc.NewServer()
	pb.RegisterInventoryServiceServer(server, NewGRPCHandler(svc))

	log.Printf("gRPC server starting on :%s", port)

	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	return server, nil
}

// ---- Kafka Handler ----

// KafkaHandler dispatches Kafka messages to the service layer.
// It unmarshals the event envelope and delegates to StockService — no Redis logic here.
type KafkaHandler struct {
	svc StockService
}

func NewKafkaHandler(svc StockService) *KafkaHandler {
	return &KafkaHandler{svc: svc}
}

// Handle is the kafka.MessageHandler signature.
// Return nil to commit the offset; return error to log and skip (no retry).
func (h *KafkaHandler) Handle(key string, value []byte) error {
	var event models.OrderEvent
	if err := json.Unmarshal(value, &event); err != nil {
		return fmt.Errorf("failed to unmarshal event: %w", err)
	}

	switch event.EventType {
	case "order.created":
		ctx, cancel := context.WithTimeout(context.Background(), 5e9) // 5s
		defer cancel()
		return h.svc.ProcessOrder(ctx, event.Order)
	default:
		log.Printf("Unknown event type: %s, skipping", event.EventType)
		return nil
	}
}
