package booking

import (
	"context"

	"github.com/Oregon-MAI/oregon-booking-service/internal/domain/models"
	bookingv1 "github.com/Oregon-MAI/oregon-infrastructure/contracts/gen/go/booking"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type BookingService interface {
	CreateBooking(ctx context.Context, in CreateBookingRequest) (*models.Booking, error)
	GetBooking(ctx context.Context, bookingID string) (*models.Booking, error)
	UserCancelBooking(ctx context.Context, bookingID string) (*models.Booking, error)
	AdminCancelBooking(ctx context.Context, bookingID string) (*models.Booking, error)
	ListBookingsByUser(ctx context.Context, userID string) ([]*models.Booking, error)
	ListBookingsByResource(ctx context.Context, in ListBookingsByResourceRequest) ([]*models.Booking, error)
}

type ServerAPI struct {
	bookingv1.UnimplementedBookingServiceServer
	service BookingService
}

func NewServer(gRPCServer *grpc.Server, bookingService BookingService) {
	bookingv1.RegisterBookingServiceServer(gRPCServer, &ServerAPI{service: bookingService})
}

func (s *ServerAPI) CreateBooking(ctx context.Context, in *bookingv1.CreateBookingRequest) (*bookingv1.CreateBookingResponse, error) {
	if in.GetResourceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "resource id is required")
	}
	if in.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user id is required")
	}
	if in.GetStartsAt() == nil {
		return nil, status.Error(codes.InvalidArgument, "start time is required")
	}
	if in.GetEndsAt() == nil {
		return nil, status.Error(codes.InvalidArgument, "end time is required")
	}

	req := CreateBookingRequest{
		ResourceID: in.GetResourceId(),
		UserID:     in.GetUserId(),
		StartsAt:   in.GetStartsAt().AsTime(),
		EndsAt:     in.GetEndsAt().AsTime(),
	}

	booking, err := s.service.CreateBooking(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to create booking: "+err.Error())
	}

	return &bookingv1.CreateBookingResponse{
		Booking: mapServiceBookingToProto(booking),
	}, nil
}

func (s *ServerAPI) GetBooking(ctx context.Context, in *bookingv1.GetBookingRequest) (*bookingv1.GetBookingResponse, error) {
	if in.GetBookingId() == "" {
		return nil, status.Error(codes.InvalidArgument, "booking id is required")
	}

	booking, err := s.service.GetBooking(ctx, in.GetBookingId())
	if err != nil {
		//TODO : distinguish between not found and internal error
		return nil, status.Error(codes.NotFound, "booking not found")
	}

	return &bookingv1.GetBookingResponse{
		Booking: mapServiceBookingToProto(booking),
	}, nil
}

func (s *ServerAPI) UserCancelBooking(ctx context.Context, in *bookingv1.UserCancelBookingRequest) (*bookingv1.UserCancelBookingResponse, error) {
	if in.GetBookingId() == "" {
		return nil, status.Error(codes.InvalidArgument, "booking id is required")
	}

	canceledBooking, err := s.service.UserCancelBooking(ctx, in.GetBookingId())
	if err != nil {
		// TODO : distinguish errors
		return nil, status.Error(codes.Internal, "failed to cancel booking: "+err.Error())
	}

	return &bookingv1.UserCancelBookingResponse{
		Booking: mapServiceBookingToProto(canceledBooking),
	}, nil
}

func (s *ServerAPI) AdminCancelBooking(ctx context.Context, in *bookingv1.AdminCancelBookingRequest) (*bookingv1.AdminCancelBookingResponse, error) {
	if in.GetBookingId() == "" {
		return nil, status.Error(codes.InvalidArgument, "booking id is required")
	}

	canceledBooking, err := s.service.AdminCancelBooking(ctx, in.GetBookingId())
	if err != nil {
		// TODO : distinguish errors
		return nil, status.Error(codes.Internal, "failed to cancel booking: "+err.Error())
	}

	return &bookingv1.AdminCancelBookingResponse{
		Booking: mapServiceBookingToProto(canceledBooking),
	}, nil
}

func (s *ServerAPI) ListBookingsByUser(ctx context.Context, in *bookingv1.ListBookingsByUserRequest) (*bookingv1.ListBookingsByUserResponse, error) {
	if in.GetUserId() == "" {
		return nil, status.Error(codes.InvalidArgument, "user id is required")
	}

	bookings, err := s.service.ListBookingsByUser(ctx, in.GetUserId())
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to get bookings by user: "+err.Error())
	}

	return &bookingv1.ListBookingsByUserResponse{Bookings: mapServiceBookingsToProto(bookings)}, nil
}

func (s *ServerAPI) ListBookingsByResource(ctx context.Context, in *bookingv1.ListBookingsByResourceRequest) (*bookingv1.ListBookingsByResourceResponse, error) {
	if in.GetResourceId() == "" {
		return nil, status.Error(codes.InvalidArgument, "resource id is required")
	}
	if in.GetFrom() == nil {
		return nil, status.Error(codes.InvalidArgument, "from is required")
	}
	if in.GetTo() == nil {
		return nil, status.Error(codes.InvalidArgument, "to is required")
	}

	req := ListBookingsByResourceRequest{
		ResourceID: in.GetResourceId(),
		From:       in.GetFrom().AsTime(),
		To:         in.GetTo().AsTime(),
	}

	bookings, err := s.service.ListBookingsByResource(ctx, req)
	if err != nil {
		return nil, status.Error(codes.Internal, "failed to list bookings by resource: "+err.Error())
	}

	return &bookingv1.ListBookingsByResourceResponse{Bookings: mapServiceBookingsToProto(bookings)}, nil
}

///////////////////////////////////////

func mapServiceBookingToProto(booking *models.Booking) *bookingv1.Booking {
	return &bookingv1.Booking{
		BookingId:        booking.BookingID,
		UserId:           booking.UserID,
		ResourceId:       booking.ResourceID,
		ResourceType:     booking.ResourceType,
		ResourceLocation: booking.ResourceLocation,
		ResourceName:     booking.ResourceName,
		StartsAt:         timestamppb.New(booking.StartsAt),
		EndsAt:           timestamppb.New(booking.EndsAt),
		Status:           ServiceStatusToProto(booking.Status),
		CancelReason:     booking.CancelReason,
		CreatedAt:        timestamppb.New(booking.CreatedAt),
		UpdatedAt:        timestamppb.New(booking.UpdatedAt),
	}
}

func mapServiceBookingsToProto(bookings []*models.Booking) []*bookingv1.Booking {
	out := make([]*bookingv1.Booking, 0, len(bookings))
	for _, booking := range bookings {
		out = append(out, mapServiceBookingToProto(booking))
	}

	return out
}

func ServiceStatusToProto(status models.BookingStatus) bookingv1.BookingStatus {
	switch status {
	case models.BookingStatusConfirmed:
		return bookingv1.BookingStatus_BOOKING_STATUS_CONFIRMED
	case models.BookingStatusCanceled:
		return bookingv1.BookingStatus_BOOKING_STATUS_CANCELED
	default:
		return bookingv1.BookingStatus_BOOKING_STATUS_UNSPECIFIED
	}
}
