package service

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Oregon-MAI/oregon-booking-service/internal/domain/events"
	"github.com/Oregon-MAI/oregon-booking-service/internal/domain/models"
	"github.com/Oregon-MAI/oregon-booking-service/internal/grpc/booking"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

const (
	resourceTypeMeetingRoom = "meeting_room"
	resourceTypeWorkspace   = "workspace"
	resourceStatusAvailable = "available"
	bookingGap              = 15 * time.Minute
	defaultUserBookingTopic = "topic.user.booking"
	defaultAdminCancelTopic = "topic.admin.cancel"
	defaultUserCancelTopic  = "topic.user.cancel"
	defaultRemindStartTopic = "topic.messages.start"
	defaultRemindEndTopic   = "topic.messages.end"
	reminderAdvance         = 15 * time.Minute
	headerUserID            = "x-user-id"
	headerUserRole          = "x-user-role"
	roleAdmin               = "admin"
)

type authClaims struct {
	UserID string
	Roles  map[string]struct{}
}

type Resource struct {
	ID       string
	Name     string
	Type     string
	Location string
	Status   string
}

type Repository interface {
	CreateBooking(ctx context.Context, booking *models.Booking, outbox []*models.OutboxMessage) (*models.Booking, error)
	GetBooking(ctx context.Context, bookingID string) (*models.Booking, error)
	CancelBooking(ctx context.Context, bookingID string) (*models.Booking, error)
	ListBookingsByUser(ctx context.Context, userID string) ([]*models.Booking, error)
	ListBookingsByResource(ctx context.Context, resourceID string, from time.Time, to time.Time) ([]*models.Booking, error)
	HasBookingConflict(ctx context.Context, userID string, resourceType string, startsAt time.Time, endsAt time.Time, gap time.Duration) (bool, error)
}

type ResourceClient interface {
	GetResource(ctx context.Context, resourceID string) (*Resource, error)
}

type EventProducer interface {
	ProduceEvent(ctx context.Context, topic string, key string, msg any) error
}

type EventTopics struct {
	UserBooking string
	AdminCancel string
	UserCancel  string
	RemindStart string
	RemindEnd   string
}

type Service struct {
	log            *slog.Logger
	tracer         trace.Tracer
	repo           Repository
	resourceClient ResourceClient
	producer       EventProducer
	topics         EventTopics
}

func NewService(log *slog.Logger, repo Repository, resourceClient ResourceClient, producer EventProducer, topics EventTopics) *Service {
	if log == nil {
		log = slog.Default()
	}

	if strings.TrimSpace(topics.UserBooking) == "" {
		topics.UserBooking = defaultUserBookingTopic
	}
	if strings.TrimSpace(topics.AdminCancel) == "" {
		topics.AdminCancel = defaultAdminCancelTopic
	}
	if strings.TrimSpace(topics.UserCancel) == "" {
		topics.UserCancel = defaultUserCancelTopic
	}
	if strings.TrimSpace(topics.RemindStart) == "" {
		topics.RemindStart = defaultRemindStartTopic
	}
	if strings.TrimSpace(topics.RemindEnd) == "" {
		topics.RemindEnd = defaultRemindEndTopic
	}

	return &Service{
		log:            log,
		tracer:         otel.GetTracerProvider().Tracer("booking/service"),
		repo:           repo,
		resourceClient: resourceClient,
		producer:       producer,
		topics:         topics,
	}
}

func (s *Service) CreateBooking(ctx context.Context, in booking.CreateBookingRequest) (*models.Booking, error) {
	const op = "Service.CreateBooking"

	ctx, span := s.tracer.Start(ctx, op)
	defer span.End()

	if in.StartsAt.IsZero() || in.EndsAt.IsZero() || !in.StartsAt.Before(in.EndsAt) {
		return nil, fmt.Errorf("%s: invalid booking time range", op)
	}

	if !in.StartsAt.After(time.Now().UTC()) {
		return nil, fmt.Errorf("%s: booking start time must be in the future", op)
	}

	if s.resourceClient == nil {
		return nil, fmt.Errorf("%s: resource client is not configured", op)
	}

	resource, err := s.resourceClient.GetResource(ctx, in.ResourceID)
	if err != nil {
		s.log.ErrorContext(ctx, "get resource failed", slog.String("resource_id", in.ResourceID), slog.Any("error", err))
		return nil, fmt.Errorf("%s: get resource: %w", op, err)
	}

	if !strings.EqualFold(resource.Status, resourceStatusAvailable) {
		return nil, fmt.Errorf("%s: resource is not available", op)
	}

	if requiresConflictCheck(resource.Type) {
		conflict, err := s.repo.HasBookingConflict(ctx, in.UserID, resource.Type, in.StartsAt, in.EndsAt, bookingGap)
		if err != nil {
			s.log.ErrorContext(ctx, "conflict check failed", slog.String("resource_id", in.ResourceID), slog.Any("error", err))
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		if conflict {
			return nil, fmt.Errorf("%s: booking time conflicts with existing bookings", op)
		}
	}

	b := &models.Booking{
		ResourceID:       in.ResourceID,
		UserID:           in.UserID,
		ResourceName:     resource.Name,
		ResourceType:     resource.Type,
		ResourceLocation: resource.Location,
		StartsAt:         in.StartsAt,
		EndsAt:           in.EndsAt,
		Status:           models.BookingStatusConfirmed,
	}

	outboxMessages, err := s.buildReminderOutboxes(b)
	if err != nil {
		s.log.WarnContext(ctx, "build reminder outbox failed", slog.Any("error", err))
	}

	created, err := s.repo.CreateBooking(ctx, b, outboxMessages)
	if err != nil {
		s.log.ErrorContext(ctx, "create booking failed",
			slog.String("resource_id", in.ResourceID),
			slog.String("user_id", in.UserID),
			slog.Any("error", err),
		)
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	s.log.InfoContext(ctx, "booking created",
		slog.String("booking_id", created.BookingID),
		slog.String("resource_id", created.ResourceID),
		slog.String("user_id", created.UserID),
	)

	s.produceUserBookingEvent(ctx, created)

	return created, nil
}

func (s *Service) buildReminderOutboxes(b *models.Booking) ([]*models.OutboxMessage, error) {
	if s == nil || s.producer == nil || b == nil {
		return nil, nil
	}

	reminder := events.BookingReminder{
		ToUser:    b.UserID,
		StartTime: b.StartsAt,
		EndTime:   b.EndsAt,
		Location:  b.ResourceLocation,
		Type:      b.ResourceType,
		Name:      b.ResourceName,
	}
	payload, err := json.Marshal(reminder)
	if err != nil {
		return nil, fmt.Errorf("build reminder payload: %w", err)
	}

	now := time.Now().UTC()
	outboxMessages := make([]*models.OutboxMessage, 0, 2)

	startAt := b.StartsAt.Add(-reminderAdvance)
	if startAt.After(now) {
		outboxMessages = append(outboxMessages, &models.OutboxMessage{
			BookingID:   b.BookingID,
			Topic:       s.topics.RemindStart,
			Key:         b.UserID,
			Payload:     payload,
			ScheduledAt: startAt,
		})
	}

	endAt := b.EndsAt.Add(-reminderAdvance)
	if endAt.After(now) {
		outboxMessages = append(outboxMessages, &models.OutboxMessage{
			BookingID:   b.BookingID,
			Topic:       s.topics.RemindEnd,
			Key:         b.UserID,
			Payload:     payload,
			ScheduledAt: endAt,
		})
	}

	if len(outboxMessages) == 0 {
		return nil, nil
	}

	return outboxMessages, nil
}

func (s *Service) GetBooking(ctx context.Context, bookingID string) (*models.Booking, error) {
	const op = "Service.GetBooking"

	ctx, span := s.tracer.Start(ctx, op)
	defer span.End()

	b, err := s.repo.GetBooking(ctx, bookingID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return b, nil
}

func (s *Service) UserCancelBooking(ctx context.Context, bookingID string) (*models.Booking, error) {
	const op = "Service.UserCancelBooking"

	ctx, span := s.tracer.Start(ctx, op)
	defer span.End()

	claims, err := authFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	targetBooking, err := s.repo.GetBooking(ctx, bookingID)
	if err != nil {
		s.log.ErrorContext(ctx, "get booking before user cancel failed", slog.String("booking_id", bookingID), slog.Any("error", err))
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	if targetBooking == nil {
		return nil, fmt.Errorf("%s: booking not found", op)
	}

	if !strings.EqualFold(targetBooking.UserID, claims.UserID) {
		return nil, fmt.Errorf("%s: %w", op, booking.ErrPermissionDenied)
	}

	b, err := s.repo.CancelBooking(ctx, bookingID)
	if err != nil {
		s.log.ErrorContext(ctx, "user cancel booking failed", slog.String("booking_id", bookingID), slog.Any("error", err))
		return nil, fmt.Errorf("%s: db call: %w", op, err)
	}

	s.log.InfoContext(ctx, "booking canceled by user", slog.String("booking_id", b.BookingID), slog.String("user_id", b.UserID))
	s.produceUserCancelEvent(ctx, b)

	return b, nil
}

func (s *Service) AdminCancelBooking(ctx context.Context, bookingID string) (*models.Booking, error) {
	const op = "Service.AdminCancelBooking"

	ctx, span := s.tracer.Start(ctx, op)
	defer span.End()

	claims, err := authFromContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	if !hasRole(claims.Roles, roleAdmin) {
		return nil, fmt.Errorf("%s: %w", op, booking.ErrPermissionDenied)
	}

	b, err := s.repo.CancelBooking(ctx, bookingID)
	if err != nil {
		s.log.ErrorContext(ctx, "admin cancel booking failed", slog.String("booking_id", bookingID), slog.Any("error", err))
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	s.log.InfoContext(ctx, "booking canceled by admin", slog.String("booking_id", b.BookingID), slog.String("user_id", b.UserID))
	s.produceAdminCancelEvent(ctx, b)

	return b, nil
}

func (s *Service) ListBookingsByUser(ctx context.Context, userID string) ([]*models.Booking, error) {
	const op = "Service.ListBookingsByUser"

	ctx, span := s.tracer.Start(ctx, op)
	defer span.End()

	bookings, err := s.repo.ListBookingsByUser(ctx, userID)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return bookings, nil
}

func (s *Service) ListBookingsByResource(ctx context.Context, in booking.ListBookingsByResourceRequest) ([]*models.Booking, error) {
	const op = "Service.ListBookingsByResource"

	ctx, span := s.tracer.Start(ctx, op)
	defer span.End()

	if in.From.IsZero() || in.To.IsZero() || !in.From.Before(in.To) {
		return nil, fmt.Errorf("%s: invalid time range", op)
	}

	bookings, err := s.repo.ListBookingsByResource(ctx, in.ResourceID, in.From, in.To)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return bookings, nil
}

func requiresConflictCheck(resourceType string) bool {
	resourceType = strings.ToLower(resourceType)
	return resourceType == resourceTypeMeetingRoom || resourceType == resourceTypeWorkspace
}

func (s *Service) produceUserBookingEvent(ctx context.Context, b *models.Booking) {
	if s.producer == nil || b == nil {
		return
	}

	err := s.producer.ProduceEvent(ctx, s.topics.UserBooking, b.UserID, events.UserBooking{
		ToUser:    b.UserID,
		Status:    string(b.Status),
		StartTime: b.StartsAt,
		EndTime:   b.EndsAt,
		Location:  b.ResourceLocation,
		Type:      b.ResourceType,
		Name:      b.ResourceName,
	})
	if err != nil {
		s.log.WarnContext(ctx, "publish user book event failed", slog.String("booking_id", b.BookingID), slog.Any("error", err))
	}
}

func (s *Service) produceAdminCancelEvent(ctx context.Context, b *models.Booking) {
	if s.producer == nil || b == nil {
		return
	}

	err := s.producer.ProduceEvent(ctx, s.topics.AdminCancel, b.UserID, events.AdminCancel{
		ToUser:    b.UserID,
		Status:    string(b.Status),
		StartTime: b.StartsAt,
		EndTime:   b.EndsAt,
		Location:  b.ResourceLocation,
		Type:      b.ResourceType,
		Name:      b.ResourceName,
	})
	if err != nil {
		s.log.WarnContext(ctx, "publish admin cancel event failed", slog.String("booking_id", b.BookingID), slog.Any("error", err))
	}
}

func (s *Service) produceUserCancelEvent(ctx context.Context, b *models.Booking) {
	if s.producer == nil || b == nil {
		return
	}

	err := s.producer.ProduceEvent(ctx, s.topics.UserCancel, b.UserID, events.UserCancel{
		ToUser:    b.UserID,
		StartTime: b.StartsAt,
		EndTime:   b.EndsAt,
		Location:  b.ResourceLocation,
		Type:      b.ResourceType,
		Name:      b.ResourceName,
	})
	if err != nil {
		s.log.WarnContext(ctx, "publish user cancel event failed", slog.String("booking_id", b.BookingID), slog.Any("error", err))
	}
}

func authFromContext(ctx context.Context) (authClaims, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return authClaims{}, booking.ErrUnauthenticated
	}

	userID := strings.TrimSpace(firstMetadataValue(md, headerUserID))
	if userID == "" {
		return authClaims{}, booking.ErrUnauthenticated
	}

	roles := parseRoles(md.Get(headerUserRole))

	return authClaims{
		UserID: userID,
		Roles:  roles,
	}, nil
}

func firstMetadataValue(md metadata.MD, key string) string {
	values := md.Get(key)
	if len(values) == 0 {
		return ""
	}

	return values[0]
}

func parseRoles(values []string) map[string]struct{} {
	roles := make(map[string]struct{})
	for _, raw := range values {
		for _, role := range strings.Split(raw, ",") {
			normalized := strings.ToLower(strings.TrimSpace(role))
			if normalized == "" {
				continue
			}
			roles[normalized] = struct{}{}
		}
	}

	return roles
}

func hasRole(roles map[string]struct{}, role string) bool {
	_, ok := roles[strings.ToLower(strings.TrimSpace(role))]
	return ok
}
