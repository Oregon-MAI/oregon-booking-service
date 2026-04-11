package service

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Oregon-MAI/oregon-booking-service/internal/domain/events"
	"github.com/Oregon-MAI/oregon-booking-service/internal/domain/models"
	grpcbooking "github.com/Oregon-MAI/oregon-booking-service/internal/grpc/booking"
)

type repoMock struct {
	createFn func(ctx context.Context, booking *models.Booking) (*models.Booking, error)
	cancelFn func(ctx context.Context, bookingID string) (*models.Booking, error)
}

func (m *repoMock) CreateBooking(ctx context.Context, booking *models.Booking) (*models.Booking, error) {
	return m.createFn(ctx, booking)
}

func (m *repoMock) GetBooking(context.Context, string) (*models.Booking, error) {
	return nil, nil
}

func (m *repoMock) CancelBooking(ctx context.Context, bookingID string) (*models.Booking, error) {
	return m.cancelFn(ctx, bookingID)
}

func (m *repoMock) ListBookingsByUser(context.Context, string) ([]*models.Booking, error) {
	return nil, nil
}

func (m *repoMock) ListBookingsByResource(context.Context, string, time.Time, time.Time) ([]*models.Booking, error) {
	return nil, nil
}

func (m *repoMock) HasBookingConflict(context.Context, string, time.Time, time.Time, time.Duration) (bool, error) {
	return false, nil
}

type resourceClientMock struct{}

func (resourceClientMock) GetResource(context.Context, string) (*Resource, error) {
	return &Resource{
		ID:       "resource-1",
		Name:     "Room A",
		Type:     "meeting_room",
		Location: "floor-3",
		Status:   "available",
	}, nil
}

type publisherMock struct {
	topic string
	key   string
	msg   any
}

func (m *publisherMock) ProduceEvent(_ context.Context, topic string, key string, msg any) error {
	m.topic = topic
	m.key = key
	m.msg = msg
	return nil
}

func TestCreateBookingPublishesUserBookEvent(t *testing.T) {
	repo := &repoMock{
		createFn: func(_ context.Context, booking *models.Booking) (*models.Booking, error) {
			return &models.Booking{
				BookingID:        "booking-1",
				ResourceID:       booking.ResourceID,
				UserID:           booking.UserID,
				ResourceName:     booking.ResourceName,
				ResourceType:     booking.ResourceType,
				ResourceLocation: booking.ResourceLocation,
				StartsAt:         booking.StartsAt,
				EndsAt:           booking.EndsAt,
				Status:           models.BookingStatusConfirmed,
			}, nil
		},
	}
	pub := &publisherMock{}
	svc := NewService(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		repo,
		resourceClientMock{},
		pub,
		EventTopics{UserBooking: "topic.user.book", AdminCancel: "topic.admin.cancel", UserCancel: "topic.user.cancel"},
	)

	start := time.Now().UTC().Add(1 * time.Hour)
	_, err := svc.CreateBooking(context.Background(), grpcbooking.CreateBookingRequest{
		ResourceID: "resource-1",
		UserID:     "user-1",
		StartsAt:   start,
		EndsAt:     start.Add(30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("CreateBooking() error = %v", err)
	}

	if pub.topic != "topic.user.book" {
		t.Fatalf("unexpected topic: %s", pub.topic)
	}

	event, ok := pub.msg.(events.UserBooking)
	if !ok {
		t.Fatalf("unexpected event type: %T", pub.msg)
	}
	if event.ToUser != "user-1" {
		t.Fatalf("unexpected to_user: %s", event.ToUser)
	}
}

func TestUserCancelPublishesUserCancelEvent(t *testing.T) {
	repo := &repoMock{
		createFn: func(context.Context, *models.Booking) (*models.Booking, error) { return nil, nil },
		cancelFn: func(_ context.Context, _ string) (*models.Booking, error) {
			return &models.Booking{
				BookingID:        "booking-1",
				ResourceID:       "resource-1",
				UserID:           "user-1",
				ResourceName:     "Room A",
				ResourceType:     "meeting_room",
				ResourceLocation: "floor-3",
				StartsAt:         time.Now().UTC(),
				EndsAt:           time.Now().UTC().Add(30 * time.Minute),
				Status:           models.BookingStatusCanceled,
			}, nil
		},
	}
	pub := &publisherMock{}
	svc := NewService(
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		repo,
		resourceClientMock{},
		pub,
		EventTopics{UserBooking: "topic.user.book", AdminCancel: "topic.admin.cancel", UserCancel: "topic.user.cancel"},
	)

	_, err := svc.UserCancelBooking(context.Background(), "booking-1")
	if err != nil {
		t.Fatalf("UserCancelBooking() error = %v", err)
	}

	if pub.topic != "topic.user.cancel" {
		t.Fatalf("unexpected topic: %s", pub.topic)
	}

	if _, ok := pub.msg.(events.UserCancel); !ok {
		t.Fatalf("unexpected event type: %T", pub.msg)
	}
}
