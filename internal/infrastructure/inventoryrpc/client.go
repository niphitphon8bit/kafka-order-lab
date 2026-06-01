package inventoryrpc

import (
	"context"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/niphitphon8bit/kafka-order-lab/internal/infrastructure/pb"
)

// InventoryClient wraps the generated gRPC client.
// Order Service uses this to call Inventory Service methods.
type InventoryClient struct {
	client pb.InventoryServiceClient
	conn   *grpc.ClientConn
}

// NewInventoryClient connects to the Inventory Service gRPC server.
//
// Parameters:
//   - addr: the gRPC server address (e.g., "localhost:50051" or "inventory-service:50051")
//
// insecure.NewCredentials() = no TLS (fine for local dev, NOT for production)
func NewInventoryClient(addr string) (*InventoryClient, error) {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to inventory gRPC at %s: %w", addr, err)
	}

	client := pb.NewInventoryServiceClient(conn)
	log.Printf("gRPC client connected to inventory service at %s", addr)

	return &InventoryClient{
		client: client,
		conn:   conn,
	}, nil
}

// CheckStock asks Inventory Service if enough stock is available.
// Returns (available, currentStock, message, error)
func (c *InventoryClient) CheckStock(ctx context.Context, item string, quantity int) (bool, int, string, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	resp, err := c.client.CheckStock(ctx, &pb.CheckStockRequest{
		Item:     item,
		Quantity: int32(quantity),
	})
	if err != nil {
		return false, 0, "", fmt.Errorf("gRPC CheckStock failed: %w", err)
	}

	return resp.Available, int(resp.CurrentStock), resp.Message, nil
}

// Close shuts down the gRPC connection
func (c *InventoryClient) Close() error {
	return c.conn.Close()
}
