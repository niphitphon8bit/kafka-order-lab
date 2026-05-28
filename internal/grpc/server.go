package grpc

import (
	"context"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/niphitphon8bit/kafka-order-lab/internal/pb"
	appredis "github.com/niphitphon8bit/kafka-order-lab/internal/redis"
)

// InventoryServer implements the gRPC InventoryServiceServer interface.
//
// protoc generated:
//   type InventoryServiceServer interface {
//       CheckStock(ctx, *CheckStockRequest) (*CheckStockResponse, error)
//       GetStock(ctx, *GetStockRequest) (*GetStockResponse, error)
//   }
//
// We implement those methods with our actual Redis logic.
type InventoryServer struct {
	pb.UnimplementedInventoryServiceServer // Required: embed for forward compatibility
	redis *appredis.Client
}

// NewInventoryServer creates a gRPC server wired to Redis
func NewInventoryServer(redisClient *appredis.Client) *InventoryServer {
	return &InventoryServer{redis: redisClient}
}

// CheckStock verifies if enough stock is available.
// Called by Order Service before accepting an order.
func (s *InventoryServer) CheckStock(ctx context.Context, req *pb.CheckStockRequest) (*pb.CheckStockResponse, error) {
	log.Printf("[gRPC] CheckStock: item=%s quantity=%d", req.Item, req.Quantity)

	currentStock, err := s.redis.GetStock(ctx, req.Item)
	if err != nil {
		return nil, fmt.Errorf("failed to get stock: %w", err)
	}

	if currentStock == -1 {
		return &pb.CheckStockResponse{
			Available:    false,
			CurrentStock: 0,
			Message:      fmt.Sprintf("item '%s' not found in inventory", req.Item),
		}, nil
	}

	available := currentStock >= int(req.Quantity)
	msg := fmt.Sprintf("%d in stock", currentStock)
	if !available {
		msg = fmt.Sprintf("insufficient stock (need %d, have %d)", req.Quantity, currentStock)
	}

	return &pb.CheckStockResponse{
		Available:    available,
		CurrentStock: int32(currentStock),
		Message:      msg,
	}, nil
}

// GetStock returns all current stock levels.
func (s *InventoryServer) GetStock(ctx context.Context, req *pb.GetStockRequest) (*pb.GetStockResponse, error) {
	log.Println("[gRPC] GetStock: fetching all items")

	stocks, err := s.redis.GetAllStock(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get stocks: %w", err)
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

// Start launches the gRPC server on the given port.
// This runs in a goroutine — it blocks until the listener closes.
func Start(port string, redisClient *appredis.Client) (*grpc.Server, error) {
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on port %s: %w", port, err)
	}

	server := grpc.NewServer()
	pb.RegisterInventoryServiceServer(server, NewInventoryServer(redisClient))

	log.Printf("gRPC server starting on :%s", port)

	// Run in a goroutine so it doesn't block main()
	go func() {
		if err := server.Serve(listener); err != nil {
			log.Printf("gRPC server error: %v", err)
		}
	}()

	return server, nil
}
