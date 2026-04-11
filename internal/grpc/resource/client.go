package resource

import (
	"context"
	"fmt"
	"strings"

	"github.com/Oregon-MAI/oregon-booking-service/internal/service"
	resourcev1 "github.com/Oregon-MAI/oregon-infrastructure/contracts/gen/go/resource"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	api  resourcev1.ResourceBookingServiceClient
	conn *grpc.ClientConn
}

func NewClient(address string) (*Client, error) {
	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("resource client: %w", err)
	}

	return &Client{
		api:  resourcev1.NewResourceBookingServiceClient(conn),
		conn: conn,
	}, nil
}

func (c *Client) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}

	return c.conn.Close()
}

func (c *Client) GetResource(ctx context.Context, resourceID string) (*service.Resource, error) {
	const op = "Client.GetResource"

	resp, err := c.api.GetResource(ctx, &resourcev1.GetResourceRequest{ResourceId: resourceID})
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	resource := resp.GetResource()
	if resource == nil {
		return nil, fmt.Errorf("%s: resource not found", op)
	}

	return &service.Resource{
		ID:       resource.GetResourceId(),
		Name:     resource.GetName(),
		Type:     protoResourceTypeToDomain(resource.GetType()),
		Location: resource.GetLocation(),
		Status:   protoResourceStatusToDomain(resource.GetStatus()),
	}, nil
}

func protoResourceTypeToDomain(v resourcev1.ResourceType) string {
	name := strings.TrimPrefix(v.String(), "RESOURCE_TYPE_")
	return strings.ToLower(name)
}

func protoResourceStatusToDomain(v resourcev1.ResourceStatus) string {
	name := strings.TrimPrefix(v.String(), "RESOURCE_STATUS_")
	return strings.ToLower(name)
}
