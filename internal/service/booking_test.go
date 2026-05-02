package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/Oregon-MAI/oregon-booking-service/internal/domain/events"
	"github.com/Oregon-MAI/oregon-booking-service/internal/domain/models"
	grpcbooking "github.com/Oregon-MAI/oregon-booking-service/internal/grpc/booking"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
)

type repositoryMock struct{ mock.Mock }

func (m *repositoryMock) CreateBooking(ctx context.Context, booking *models.Booking, outbox []*models.OutboxMessage) (*models.Booking, error) {
	args := m.Called(ctx, booking, outbox)
	errVal := args.Get(1)
	var retErr error
	if errVal != nil {
		var ok bool
		retErr, ok = errVal.(error)
		if !ok {
			return nil, fmt.Errorf("unexpected error type: %T", errVal)
		}
	}
	if v := args.Get(0); v != nil {
		bookingValue, ok := v.(*models.Booking)
		if !ok {
			return nil, fmt.Errorf("unexpected booking type: %T", v)
		}

		return bookingValue, retErr
	}
	return nil, retErr
}

func (m *repositoryMock) GetBooking(ctx context.Context, bookingID string) (*models.Booking, error) {
	args := m.Called(ctx, bookingID)
	errVal := args.Get(1)
	var retErr error
	if errVal != nil {
		var ok bool
		retErr, ok = errVal.(error)
		if !ok {
			return nil, fmt.Errorf("unexpected error type: %T", errVal)
		}
	}
	if v := args.Get(0); v != nil {
		bookingValue, ok := v.(*models.Booking)
		if !ok {
			return nil, fmt.Errorf("unexpected booking type: %T", v)
		}

		return bookingValue, retErr
	}
	return nil, retErr
}

func (m *repositoryMock) CancelBooking(ctx context.Context, bookingID string) (*models.Booking, error) {
	args := m.Called(ctx, bookingID)
	errVal := args.Get(1)
	var retErr error
	if errVal != nil {
		var ok bool
		retErr, ok = errVal.(error)
		if !ok {
			return nil, fmt.Errorf("unexpected error type: %T", errVal)
		}
	}
	if v := args.Get(0); v != nil {
		bookingValue, ok := v.(*models.Booking)
		if !ok {
			return nil, fmt.Errorf("unexpected booking type: %T", v)
		}

		return bookingValue, retErr
	}
	return nil, retErr
}

func (m *repositoryMock) ListBookingsByUser(ctx context.Context, userID string) ([]*models.Booking, error) {
	args := m.Called(ctx, userID)
	errVal := args.Get(1)
	var retErr error
	if errVal != nil {
		var ok bool
		retErr, ok = errVal.(error)
		if !ok {
			return nil, fmt.Errorf("unexpected error type: %T", errVal)
		}
	}
	if v := args.Get(0); v != nil {
		bookingsValue, ok := v.([]*models.Booking)
		if !ok {
			return nil, fmt.Errorf("unexpected bookings type: %T", v)
		}

		return bookingsValue, retErr
	}
	return nil, retErr
}

func (m *repositoryMock) ListBookingsByResource(ctx context.Context, resourceID string, from, to time.Time) ([]*models.Booking, error) {
	args := m.Called(ctx, resourceID, from, to)
	errVal := args.Get(1)
	var retErr error
	if errVal != nil {
		var ok bool
		retErr, ok = errVal.(error)
		if !ok {
			return nil, fmt.Errorf("unexpected error type: %T", errVal)
		}
	}
	if v := args.Get(0); v != nil {
		bookingsValue, ok := v.([]*models.Booking)
		if !ok {
			return nil, fmt.Errorf("unexpected bookings type: %T", v)
		}

		return bookingsValue, retErr
	}
	return nil, retErr
}

func (m *repositoryMock) HasBookingConflict(ctx context.Context, userID string, startsAt, endsAt time.Time, gap time.Duration) (bool, error) {
	args := m.Called(ctx, userID, startsAt, endsAt, gap)
	return args.Bool(0), args.Error(1)
}

type resourceClientMock struct{ mock.Mock }

func (m *resourceClientMock) GetResource(ctx context.Context, resourceID string) (*Resource, error) {
	args := m.Called(ctx, resourceID)
	errVal := args.Get(1)
	var retErr error
	if errVal != nil {
		var ok bool
		retErr, ok = errVal.(error)
		if !ok {
			return nil, fmt.Errorf("unexpected error type: %T", errVal)
		}
	}
	if v := args.Get(0); v != nil {
		resourceValue, ok := v.(*Resource)
		if !ok {
			return nil, fmt.Errorf("unexpected resource type: %T", v)
		}

		return resourceValue, retErr
	}
	return nil, retErr
}

func (m *resourceClientMock) ChangeResourceStatus(ctx context.Context, resourceID string, status string, reason string) error {
	args := m.Called(ctx, resourceID, status, reason)
	return args.Error(0)
}

type producerMock struct{ mock.Mock }

func (m *producerMock) ProduceEvent(ctx context.Context, topic string, key string, msg any) error {
	args := m.Called(ctx, topic, key, msg)
	return args.Error(0)
}

func newTestService(repo *repositoryMock, rc *resourceClientMock, producer *producerMock, topics EventTopics) *Service {
	return NewService(slog.New(slog.NewTextHandler(io.Discard, nil)), repo, rc, producer, topics)
}

func authContext(userID string, roles string) context.Context {
	return metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		headerUserID, userID,
		headerUserRole, roles,
	))
}

func sampleBooking() *models.Booking {
	start := time.Date(2026, 4, 16, 10, 0, 0, 0, time.UTC)
	return &models.Booking{
		BookingID:        "booking-1",
		ResourceID:       "resource-1",
		UserID:           "user-1",
		ResourceName:     "Room A",
		ResourceType:     "meeting_room",
		ResourceLocation: "floor-3",
		StartsAt:         start,
		EndsAt:           start.Add(30 * time.Minute),
		Status:           models.BookingStatusConfirmed,
	}
}

func TestNewService_DefaultTopics(t *testing.T) {
	svc := NewService(nil, &repositoryMock{}, &resourceClientMock{}, nil, EventTopics{})
	require.Equal(t, defaultUserBookingTopic, svc.topics.UserBooking)
	require.Equal(t, defaultAdminCancelTopic, svc.topics.AdminCancel)
	require.Equal(t, defaultUserCancelTopic, svc.topics.UserCancel)
	require.Equal(t, defaultRemindStartTopic, svc.topics.RemindStart)
	require.Equal(t, defaultRemindEndTopic, svc.topics.RemindEnd)
}

func TestCreateBooking_Success(t *testing.T) {
	ctx := context.Background()
	start := time.Now().UTC().Add(1 * time.Hour)
	end := start.Add(30 * time.Minute)
	in := grpcbooking.CreateBookingRequest{ResourceID: "resource-1", UserID: "user-1", StartsAt: start, EndsAt: end}

	repo := &repositoryMock{}
	rc := &resourceClientMock{}
	prod := &producerMock{}
	topics := EventTopics{
		UserBooking: "topic.user.book",
		RemindStart: "topic.messages.start",
		RemindEnd:   "topic.messages.end",
	}

	rc.On("GetResource", mock.Anything, "resource-1").Return(&Resource{ID: "resource-1", Name: "Room A", Type: "meeting_room", Location: "floor-3", Status: "available"}, nil).Once()
	repo.On("HasBookingConflict", mock.Anything, "user-1", start, end, bookingGap).Return(false, nil).Once()
	repo.On("CreateBooking", mock.Anything, mock.Anything, mock.MatchedBy(func(outbox []*models.OutboxMessage) bool {
		if len(outbox) != 2 {
			return false
		}
		topics := map[string]struct{}{outbox[0].Topic: {}, outbox[1].Topic: {}}
		_, hasStart := topics["topic.messages.start"]
		_, hasEnd := topics["topic.messages.end"]
		return hasStart && hasEnd
	})).Return(sampleBooking(), nil).Once()
	prod.On("ProduceEvent", mock.Anything, "topic.user.book", "user-1", mock.MatchedBy(func(msg any) bool {
		_, ok := msg.(events.UserBooking)
		return ok
	})).Return(nil).Once()

	svc := newTestService(repo, rc, prod, topics)
	out, err := svc.CreateBooking(ctx, in)
	require.NoError(t, err)
	require.Equal(t, "booking-1", out.BookingID)

	repo.AssertExpectations(t)
	rc.AssertExpectations(t)
	prod.AssertExpectations(t)
}

func TestCreateBooking_ReminderSkippedWhenTooSoon(t *testing.T) {
	ctx := context.Background()
	start := time.Now().UTC().Add(10 * time.Minute)
	end := start.Add(30 * time.Minute)
	in := grpcbooking.CreateBookingRequest{ResourceID: "resource-1", UserID: "user-1", StartsAt: start, EndsAt: end}

	repo := &repositoryMock{}
	rc := &resourceClientMock{}
	prod := &producerMock{}

	rc.On("GetResource", mock.Anything, "resource-1").Return(&Resource{ID: "resource-1", Name: "Room A", Type: "meeting_room", Location: "floor-3", Status: "available"}, nil).Once()
	repo.On("HasBookingConflict", mock.Anything, "user-1", start, end, bookingGap).Return(false, nil).Once()
	repo.On("CreateBooking", mock.Anything, mock.Anything, mock.MatchedBy(func(outbox []*models.OutboxMessage) bool {
		return len(outbox) == 1 && outbox[0].Topic == defaultRemindEndTopic
	})).Return(sampleBooking(), nil).Once()
	prod.On("ProduceEvent", mock.Anything, defaultUserBookingTopic, "user-1", mock.Anything).Return(nil).Once()

	svc := newTestService(repo, rc, prod, EventTopics{})
	_, err := svc.CreateBooking(ctx, in)
	require.NoError(t, err)

	repo.AssertExpectations(t)
	rc.AssertExpectations(t)
	prod.AssertExpectations(t)
}

func TestUserCancelBooking_Success(t *testing.T) {
	ctx := authContext("user-1", "user")
	cancelled := sampleBooking()
	cancelled.Status = models.BookingStatusCanceled

	repo := &repositoryMock{}
	rc := &resourceClientMock{}
	prod := &producerMock{}

	repo.On("GetBooking", mock.Anything, "booking-1").Return(sampleBooking(), nil).Once()
	repo.On("CancelBooking", mock.Anything, "booking-1").Return(cancelled, nil).Once()
	prod.On("ProduceEvent", mock.Anything, "topic.user.cancel", "user-1", mock.MatchedBy(func(msg any) bool {
		_, ok := msg.(events.UserCancel)
		return ok
	})).Return(nil).Once()

	svc := newTestService(repo, rc, prod, EventTopics{UserCancel: "topic.user.cancel"})
	_, err := svc.UserCancelBooking(ctx, "booking-1")
	require.NoError(t, err)

	repo.AssertExpectations(t)
	rc.AssertExpectations(t)
	prod.AssertExpectations(t)
}

func TestListBookingsByResource_InvalidRange(t *testing.T) {
	svc := newTestService(&repositoryMock{}, &resourceClientMock{}, &producerMock{}, EventTopics{})
	_, err := svc.ListBookingsByResource(context.Background(), grpcbooking.ListBookingsByResourceRequest{
		ResourceID: "resource-1",
		From:       time.Now().UTC(),
		To:         time.Now().UTC().Add(-1 * time.Minute),
	})
	require.Error(t, err)
}

func TestCreateBooking_InvalidRange(t *testing.T) {
	svc := newTestService(&repositoryMock{}, &resourceClientMock{}, &producerMock{}, EventTopics{})
	now := time.Now().UTC()

	_, err := svc.CreateBooking(context.Background(), grpcbooking.CreateBookingRequest{
		ResourceID: "resource-1",
		UserID:     "user-1",
		StartsAt:   now,
		EndsAt:     now,
	})
	require.Error(t, err)
}

func TestCreateBooking_StartInPast(t *testing.T) {
	svc := newTestService(&repositoryMock{}, &resourceClientMock{}, &producerMock{}, EventTopics{})
	now := time.Now().UTC()

	_, err := svc.CreateBooking(context.Background(), grpcbooking.CreateBookingRequest{
		ResourceID: "resource-1",
		UserID:     "user-1",
		StartsAt:   now.Add(-1 * time.Minute),
		EndsAt:     now.Add(10 * time.Minute),
	})
	require.Error(t, err)
}

func TestCreateBooking_ResourceClientNil(t *testing.T) {
	svc := NewService(nil, &repositoryMock{}, nil, nil, EventTopics{})
	start := time.Now().UTC().Add(1 * time.Hour)

	_, err := svc.CreateBooking(context.Background(), grpcbooking.CreateBookingRequest{
		ResourceID: "resource-1",
		UserID:     "user-1",
		StartsAt:   start,
		EndsAt:     start.Add(30 * time.Minute),
	})
	require.Error(t, err)
}

func TestCreateBooking_GetResourceError(t *testing.T) {
	repo := &repositoryMock{}
	rc := &resourceClientMock{}
	prod := &producerMock{}
	start := time.Now().UTC().Add(1 * time.Hour)
	wantErr := errors.New("get resource failed")

	rc.On("GetResource", mock.Anything, "resource-1").Return((*Resource)(nil), wantErr).Once()

	svc := newTestService(repo, rc, prod, EventTopics{})
	_, err := svc.CreateBooking(context.Background(), grpcbooking.CreateBookingRequest{
		ResourceID: "resource-1",
		UserID:     "user-1",
		StartsAt:   start,
		EndsAt:     start.Add(30 * time.Minute),
	})
	require.ErrorIs(t, err, wantErr)
}

func TestCreateBooking_ResourceNotAvailable(t *testing.T) {
	repo := &repositoryMock{}
	rc := &resourceClientMock{}
	prod := &producerMock{}
	start := time.Now().UTC().Add(1 * time.Hour)

	rc.On("GetResource", mock.Anything, "resource-1").Return(&Resource{ID: "resource-1", Status: "maintenance", Type: "meeting_room"}, nil).Once()

	svc := newTestService(repo, rc, prod, EventTopics{})
	_, err := svc.CreateBooking(context.Background(), grpcbooking.CreateBookingRequest{
		ResourceID: "resource-1",
		UserID:     "user-1",
		StartsAt:   start,
		EndsAt:     start.Add(30 * time.Minute),
	})
	require.Error(t, err)
	repo.AssertNotCalled(t, "HasBookingConflict", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestCreateBooking_ConflictBranches(t *testing.T) {
	start := time.Now().UTC().Add(1 * time.Hour)
	end := start.Add(30 * time.Minute)
	in := grpcbooking.CreateBookingRequest{ResourceID: "resource-1", UserID: "user-1", StartsAt: start, EndsAt: end}
	resource := &Resource{ID: "resource-1", Name: "Room A", Type: "meeting_room", Location: "floor-3", Status: "available"}

	t.Run("conflict check error", func(t *testing.T) {
		repo := &repositoryMock{}
		rc := &resourceClientMock{}
		prod := &producerMock{}
		wantErr := errors.New("conflict check failed")

		rc.On("GetResource", mock.Anything, "resource-1").Return(resource, nil).Once()
		repo.On("HasBookingConflict", mock.Anything, "user-1", start, end, bookingGap).Return(false, wantErr).Once()

		svc := newTestService(repo, rc, prod, EventTopics{})
		_, err := svc.CreateBooking(context.Background(), in)
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("conflict detected", func(t *testing.T) {
		repo := &repositoryMock{}
		rc := &resourceClientMock{}
		prod := &producerMock{}

		rc.On("GetResource", mock.Anything, "resource-1").Return(resource, nil).Once()
		repo.On("HasBookingConflict", mock.Anything, "user-1", start, end, bookingGap).Return(true, nil).Once()

		svc := newTestService(repo, rc, prod, EventTopics{})
		_, err := svc.CreateBooking(context.Background(), in)
		require.Error(t, err)
		repo.AssertNotCalled(t, "CreateBooking", mock.Anything, mock.Anything, mock.Anything)
	})
}

func TestCreateBooking_RepositoryCreateError(t *testing.T) {
	start := time.Now().UTC().Add(1 * time.Hour)
	end := start.Add(30 * time.Minute)
	wantErr := errors.New("insert failed")

	repo := &repositoryMock{}
	rc := &resourceClientMock{}
	prod := &producerMock{}

	rc.On("GetResource", mock.Anything, "resource-1").Return(&Resource{ID: "resource-1", Type: "meeting_room", Status: "available"}, nil).Once()
	repo.On("HasBookingConflict", mock.Anything, "user-1", start, end, bookingGap).Return(false, nil).Once()
	repo.On("CreateBooking", mock.Anything, mock.Anything, mock.Anything).Return((*models.Booking)(nil), wantErr).Once()

	svc := newTestService(repo, rc, prod, EventTopics{})
	_, err := svc.CreateBooking(context.Background(), grpcbooking.CreateBookingRequest{
		ResourceID: "resource-1",
		UserID:     "user-1",
		StartsAt:   start,
		EndsAt:     end,
	})
	require.ErrorIs(t, err, wantErr)
	rc.AssertNotCalled(t, "ChangeResourceStatus", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestCreateBooking_DeviceSkipsConflictCheck(t *testing.T) {
	start := time.Now().UTC().Add(1 * time.Hour)
	end := start.Add(30 * time.Minute)

	repo := &repositoryMock{}
	rc := &resourceClientMock{}
	prod := &producerMock{}
	b := sampleBooking()
	b.ResourceType = "device"

	rc.On("GetResource", mock.Anything, "resource-1").Return(&Resource{ID: "resource-1", Type: "device", Status: "available"}, nil).Once()
	repo.On("CreateBooking", mock.Anything, mock.Anything, mock.Anything).Return(b, nil).Once()
	prod.On("ProduceEvent", mock.Anything, defaultUserBookingTopic, "user-1", mock.Anything).Return(nil).Once()

	svc := newTestService(repo, rc, prod, EventTopics{})
	_, err := svc.CreateBooking(context.Background(), grpcbooking.CreateBookingRequest{
		ResourceID: "resource-1",
		UserID:     "user-1",
		StartsAt:   start,
		EndsAt:     end,
	})
	require.NoError(t, err)
	repo.AssertNotCalled(t, "HasBookingConflict", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)

}

func TestCreateBooking_OccupiedStatusRejected(t *testing.T) {
	start := time.Now().UTC().Add(2 * time.Hour)
	end := start.Add(30 * time.Minute)

	repo := &repositoryMock{}
	rc := &resourceClientMock{}
	prod := &producerMock{}

	rc.On("GetResource", mock.Anything, "resource-1").Return(&Resource{ID: "resource-1", Type: "meeting_room", Status: "occupied"}, nil).Once()

	svc := newTestService(repo, rc, prod, EventTopics{})
	_, err := svc.CreateBooking(context.Background(), grpcbooking.CreateBookingRequest{
		ResourceID: "resource-1",
		UserID:     "user-1",
		StartsAt:   start,
		EndsAt:     end,
	})
	require.Error(t, err)
	repo.AssertNotCalled(t, "HasBookingConflict", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	repo.AssertNotCalled(t, "CreateBooking", mock.Anything, mock.Anything, mock.Anything)
	prod.AssertNotCalled(t, "ProduceEvent", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestGetBooking_SuccessAndError(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo := &repositoryMock{}
		repo.On("GetBooking", mock.Anything, "booking-1").Return(sampleBooking(), nil).Once()
		svc := newTestService(repo, &resourceClientMock{}, &producerMock{}, EventTopics{})

		out, err := svc.GetBooking(context.Background(), "booking-1")
		require.NoError(t, err)
		require.Equal(t, "booking-1", out.BookingID)
	})

	t.Run("error", func(t *testing.T) {
		repo := &repositoryMock{}
		wantErr := errors.New("get failed")
		repo.On("GetBooking", mock.Anything, "booking-1").Return((*models.Booking)(nil), wantErr).Once()
		svc := newTestService(repo, &resourceClientMock{}, &producerMock{}, EventTopics{})

		_, err := svc.GetBooking(context.Background(), "booking-1")
		require.ErrorIs(t, err, wantErr)
	})
}

func TestListBookingsByUser_SuccessAndError(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		repo := &repositoryMock{}
		repo.On("ListBookingsByUser", mock.Anything, "user-1").Return([]*models.Booking{sampleBooking()}, nil).Once()
		svc := newTestService(repo, &resourceClientMock{}, &producerMock{}, EventTopics{})

		out, err := svc.ListBookingsByUser(context.Background(), "user-1")
		require.NoError(t, err)
		require.Len(t, out, 1)
	})

	t.Run("error", func(t *testing.T) {
		repo := &repositoryMock{}
		wantErr := errors.New("list failed")
		repo.On("ListBookingsByUser", mock.Anything, "user-1").Return(([]*models.Booking)(nil), wantErr).Once()
		svc := newTestService(repo, &resourceClientMock{}, &producerMock{}, EventTopics{})

		_, err := svc.ListBookingsByUser(context.Background(), "user-1")
		require.ErrorIs(t, err, wantErr)
	})
}

func TestListBookingsByResource_SuccessAndError(t *testing.T) {
	from := time.Now().UTC().Add(1 * time.Hour)
	to := from.Add(1 * time.Hour)

	t.Run("success", func(t *testing.T) {
		repo := &repositoryMock{}
		repo.On("ListBookingsByResource", mock.Anything, "resource-1", from, to).Return([]*models.Booking{sampleBooking()}, nil).Once()
		svc := newTestService(repo, &resourceClientMock{}, &producerMock{}, EventTopics{})

		out, err := svc.ListBookingsByResource(context.Background(), grpcbooking.ListBookingsByResourceRequest{ResourceID: "resource-1", From: from, To: to})
		require.NoError(t, err)
		require.Len(t, out, 1)
	})

	t.Run("error", func(t *testing.T) {
		repo := &repositoryMock{}
		wantErr := errors.New("list by resource failed")
		repo.On("ListBookingsByResource", mock.Anything, "resource-1", from, to).Return(([]*models.Booking)(nil), wantErr).Once()
		svc := newTestService(repo, &resourceClientMock{}, &producerMock{}, EventTopics{})

		_, err := svc.ListBookingsByResource(context.Background(), grpcbooking.ListBookingsByResourceRequest{ResourceID: "resource-1", From: from, To: to})
		require.ErrorIs(t, err, wantErr)
	})
}

func TestCancelBranches(t *testing.T) {
	t.Run("user cancel repo error", func(t *testing.T) {
		repo := &repositoryMock{}
		rc := &resourceClientMock{}
		prod := &producerMock{}
		wantErr := errors.New("cancel failed")
		repo.On("GetBooking", mock.Anything, "booking-1").Return(sampleBooking(), nil).Once()
		repo.On("CancelBooking", mock.Anything, "booking-1").Return((*models.Booking)(nil), wantErr).Once()

		svc := newTestService(repo, rc, prod, EventTopics{})
		_, err := svc.UserCancelBooking(authContext("user-1", "user"), "booking-1")
		require.ErrorIs(t, err, wantErr)
	})

	t.Run("user cancel denied for another user", func(t *testing.T) {
		repo := &repositoryMock{}
		rc := &resourceClientMock{}
		prod := &producerMock{}
		repo.On("GetBooking", mock.Anything, "booking-1").Return(sampleBooking(), nil).Once()

		svc := newTestService(repo, rc, prod, EventTopics{})
		_, err := svc.UserCancelBooking(authContext("user-2", "user"), "booking-1")
		require.ErrorIs(t, err, grpcbooking.ErrPermissionDenied)
		repo.AssertNotCalled(t, "CancelBooking", mock.Anything, mock.Anything)
	})

	t.Run("user cancel unauthenticated", func(t *testing.T) {
		svc := newTestService(&repositoryMock{}, &resourceClientMock{}, &producerMock{}, EventTopics{})
		_, err := svc.UserCancelBooking(context.Background(), "booking-1")
		require.ErrorIs(t, err, grpcbooking.ErrUnauthenticated)
	})

	t.Run("admin cancel success", func(t *testing.T) {
		repo := &repositoryMock{}
		rc := &resourceClientMock{}
		prod := &producerMock{}
		cancelled := sampleBooking()
		cancelled.Status = models.BookingStatusCanceled

		repo.On("CancelBooking", mock.Anything, "booking-1").Return(cancelled, nil).Once()
		prod.On("ProduceEvent", mock.Anything, "topic.admin.cancel", "user-1", mock.MatchedBy(func(msg any) bool {
			_, ok := msg.(events.AdminCancel)
			return ok
		})).Return(nil).Once()

		svc := newTestService(repo, rc, prod, EventTopics{AdminCancel: "topic.admin.cancel"})
		_, err := svc.AdminCancelBooking(authContext("admin-1", "admin,user"), "booking-1")
		require.NoError(t, err)
	})

	t.Run("admin cancel denied for non-admin", func(t *testing.T) {
		repo := &repositoryMock{}
		rc := &resourceClientMock{}
		prod := &producerMock{}

		svc := newTestService(repo, rc, prod, EventTopics{})
		_, err := svc.AdminCancelBooking(authContext("user-1", "user"), "booking-1")
		require.ErrorIs(t, err, grpcbooking.ErrPermissionDenied)
		repo.AssertNotCalled(t, "CancelBooking", mock.Anything, mock.Anything)
	})

	t.Run("admin cancel repo error", func(t *testing.T) {
		repo := &repositoryMock{}
		rc := &resourceClientMock{}
		prod := &producerMock{}
		wantErr := errors.New("cancel failed")
		repo.On("CancelBooking", mock.Anything, "booking-1").Return((*models.Booking)(nil), wantErr).Once()

		svc := newTestService(repo, rc, prod, EventTopics{})
		_, err := svc.AdminCancelBooking(authContext("admin-1", "admin"), "booking-1")
		require.ErrorIs(t, err, wantErr)
	})
}

func TestProduceHelpers(t *testing.T) {
	t.Run("nil producer or nil booking", func(t *testing.T) {
		svc := newTestService(&repositoryMock{}, &resourceClientMock{}, nil, EventTopics{})
		svc.produceUserBookingEvent(context.Background(), nil)
		svc.produceAdminCancelEvent(context.Background(), nil)
		svc.produceUserCancelEvent(context.Background(), nil)
	})

	t.Run("producer error ignored", func(t *testing.T) {
		prod := &producerMock{}
		b := sampleBooking()
		b.Status = models.BookingStatusCanceled
		prod.On("ProduceEvent", mock.Anything, defaultUserBookingTopic, "user-1", mock.Anything).Return(errors.New("publish failed")).Once()
		prod.On("ProduceEvent", mock.Anything, defaultAdminCancelTopic, "user-1", mock.Anything).Return(errors.New("publish failed")).Once()
		prod.On("ProduceEvent", mock.Anything, defaultUserCancelTopic, "user-1", mock.Anything).Return(errors.New("publish failed")).Once()

		svc := newTestService(&repositoryMock{}, &resourceClientMock{}, prod, EventTopics{})
		svc.produceUserBookingEvent(context.Background(), b)
		svc.produceAdminCancelEvent(context.Background(), b)
		svc.produceUserCancelEvent(context.Background(), b)
		prod.AssertExpectations(t)
	})
}
