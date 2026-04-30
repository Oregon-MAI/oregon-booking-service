package resource

import (
	"context"
	"fmt"
	"strings"

	"github.com/Oregon-MAI/oregon-booking-service/internal/service"
	resourcev1 "github.com/Oregon-MAI/oregon-infrastructure/contracts/gen/go/resource"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Client struct {
	api    resourcev1.ResourcePublicServiceClient
	conn   *grpc.ClientConn
	tracer trace.Tracer
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
		api:    resourcev1.NewResourcePublicServiceClient(conn),
		conn:   conn,
		tracer: otel.GetTracerProvider().Tracer("resource/client"),
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

	ctx, span := c.tracer.Start(ctx, "Client.ResourceService.GetResource")
	defer span.End()

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

func (c *Client) ChangeResourceStatus(ctx context.Context, resourceID string, status string, reason string) error {
	const op = "Client.ChangeResourceStatus"

	ctx, span := c.tracer.Start(ctx, "Client.ResourceService.ChangeResourceStatus")
	defer span.End()

	statusProto, err := domainResourceStatusToProto(status)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	_, err = c.api.ChangeResourceStatus(ctx, &resourcev1.ChangeResourceStatusRequest{
		ResourceId: resourceID,
		Status:     statusProto,
		Reason:     reason,
	})
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

func protoResourceTypeToDomain(v resourcev1.ResourceType) string {
	name := strings.TrimPrefix(v.String(), "RESOURCE_TYPE_")
	return strings.ToLower(name)
}

func protoResourceStatusToDomain(v resourcev1.ResourceStatus) string {
	name := strings.TrimPrefix(v.String(), "RESOURCE_STATUS_")
	return strings.ToLower(name)
}

func domainResourceStatusToProto(status string) (resourcev1.ResourceStatus, error) {
	switch strings.ToLower(strings.TrimSpace(status)) {
	case "available":
		return resourcev1.ResourceStatus_RESOURCE_STATUS_AVAILABLE, nil
	case "occupied":
		return resourcev1.ResourceStatus_RESOURCE_STATUS_OCCUPIED, nil
	case "maintenance":
		return resourcev1.ResourceStatus_RESOURCE_STATUS_MAINTENANCE, nil
	case "emergency":
		return resourcev1.ResourceStatus_RESOURCE_STATUS_EMERGENCY, nil
	default:
		return resourcev1.ResourceStatus_RESOURCE_STATUS_UNSPECIFIED, fmt.Errorf("unknown resource status: %q", status)
	}
}
