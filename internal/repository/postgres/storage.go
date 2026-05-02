package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/Oregon-MAI/oregon-booking-service/internal/domain/models"
	_ "github.com/lib/pq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const (
	dbStatusConfirmed = "confirmed"
	dbStatusCanceled  = "canceled"

	resourceTypeMeetingRoom = "meeting_room"
	resourceTypeWorkspace   = "workspace"
)

var ErrBookingNotFound = errors.New("booking not found")

type Repository struct {
	db     *sql.DB
	log    *slog.Logger
	tracer trace.Tracer
}

func New(ctx context.Context, dsn string, log *slog.Logger) (*Repository, error) {
	if log == nil {
		log = slog.Default()
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.ErrorContext(ctx, "postgres open failed", slog.Any("error", err))
		return nil, fmt.Errorf("repository.New: %w", err)
	}

	if err = db.PingContext(ctx); err != nil {
		log.ErrorContext(ctx, "postgres ping failed", slog.Any("error", err))
		if closeErr := db.Close(); closeErr != nil {
			log.ErrorContext(ctx, "postgres close after ping failed", slog.Any("error", closeErr))
			return nil, fmt.Errorf("repository.New: ping db: %w; close db: %v", err, closeErr)
		}

		return nil, fmt.Errorf("repository.New: %w", err)
	}

	return &Repository{
		db:     db,
		log:    log,
		tracer: otel.GetTracerProvider().Tracer("booking/repository"),
	}, nil
}

func (r *Repository) Close() error {
	err := r.db.Close()
	if err != nil {
		r.log.Error("postgres close failed", slog.Any("error", err))
	}

	return err
}

func (r *Repository) CreateBooking(ctx context.Context, booking *models.Booking, outbox []*models.OutboxMessage) (*models.Booking, error) {
	const op = "Repository.CreateBooking"

	ctx, span := r.tracer.Start(ctx, op)
	defer span.End()

	if booking == nil {
		return nil, fmt.Errorf("%s: booking is nil", op)
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: begin tx: %w", op, err)
	}

	created, err := r.createBookingTransaction(ctx, tx, booking)
	if err != nil {
		r.rollbackTransaction(ctx, tx, op)
		return nil, err
	}

	if len(outbox) > 0 {
		for _, msg := range outbox {
			if msg != nil {
				msg.BookingID = created.BookingID
			}
		}
		if err := r.insertOutboxBatchTransaction(ctx, tx, outbox); err != nil {
			r.rollbackTransaction(ctx, tx, op)
			return nil, err
		}
	}

	if err := tx.Commit(); err != nil {
		r.rollbackTransaction(ctx, tx, op)
		return nil, fmt.Errorf("%s: commit tx: %w", op, err)
	}

	return created, nil
}

func (r *Repository) createBookingTransaction(ctx context.Context, tx *sql.Tx, booking *models.Booking) (*models.Booking, error) {
	const op = "Repository.createBookingTransaction"

	created := &models.Booking{}
	status := domainStatusToDB(booking.Status)
	if status == "" {
		status = dbStatusConfirmed
	}

	err := tx.QueryRowContext(ctx, `
		INSERT INTO bookings (
			resource_id,
			user_id,
			resource_name,
			resource_location,
			resource_type,
			starts_at,
			ends_at,
			status,
			cancel_reason
		)
		VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8::booking_status, $9)
		RETURNING
			booking_id::text,
			resource_id::text,
			user_id::text,
			resource_name,
			resource_location,
			resource_type,
			starts_at,
			ends_at,
			status::text,
			COALESCE(cancel_reason, ''),
			created_at,
			updated_at
	`, booking.ResourceID, booking.UserID, booking.ResourceName, booking.ResourceLocation, booking.ResourceType,
		booking.StartsAt, booking.EndsAt, status, nullableString(booking.CancelReason)).Scan(
		&created.BookingID,
		&created.ResourceID,
		&created.UserID,
		&created.ResourceName,
		&created.ResourceLocation,
		&created.ResourceType,
		&created.StartsAt,
		&created.EndsAt,
		&status,
		&created.CancelReason,
		&created.CreatedAt,
		&created.UpdatedAt,
	)
	if err != nil {
		r.log.ErrorContext(ctx, "create booking insert failed", slog.Any("error", err))
		return nil, fmt.Errorf("%s: insert booking: %w", op, err)
	}

	created.Status = dbStatusToDomain(status)
	return created, nil
}

func (r *Repository) insertOutboxBatchTransaction(ctx context.Context, tx *sql.Tx, outboxMessages []*models.OutboxMessage) error {
	const op = "Repository.insertOutboxBatchTransaction"

	for _, outbox := range outboxMessages {
		if outbox == nil {
			continue
		}
		if strings.TrimSpace(outbox.Topic) == "" {
			return fmt.Errorf("%s: outbox topic is empty", op)
		}
		if strings.TrimSpace(outbox.BookingID) == "" {
			return fmt.Errorf("%s: outbox booking_id is empty", op)
		}
		if len(outbox.Payload) == 0 {
			return fmt.Errorf("%s: outbox payload is empty", op)
		}
		if outbox.ScheduledAt.IsZero() {
			return fmt.Errorf("%s: outbox scheduled_at is empty", op)
		}

		if err := tx.QueryRowContext(ctx, `
			INSERT INTO outbox_messages (
				booking_id,
				topic,
				message_key,
				payload,
				scheduled_at
			)
			VALUES ($1::uuid, $2, $3, $4::jsonb, $5)
			RETURNING outbox_id::text
		`, outbox.BookingID, outbox.Topic, nullableString(outbox.Key), outbox.Payload, outbox.ScheduledAt).Scan(&outbox.OutboxID); err != nil {
			return fmt.Errorf("%s: insert outbox: %w", op, err)
		}
	}

	return nil
}

func (r *Repository) rollbackTransaction(ctx context.Context, tx *sql.Tx, op string) {
	if tx == nil {
		return
	}
	if err := tx.Rollback(); err != nil {
		r.log.ErrorContext(ctx, "tx rollback failed", slog.String("op", op), slog.Any("error", err))
	}
}

func (r *Repository) ListDueOutbox(ctx context.Context, now time.Time, limit int) ([]*models.OutboxMessage, error) {
	const op = "Repository.ListDueOutbox"

	ctx, span := r.tracer.Start(ctx, op)
	defer span.End()

	if limit <= 0 {
		limit = 50
	}

	rows, err := r.db.QueryContext(ctx, `
		SELECT
			outbox_id::text,
			booking_id::text,
			topic,
			COALESCE(message_key, ''),
			payload,
			scheduled_at,
			sent_at,
			attempts,
			COALESCE(last_error, '')
		FROM outbox_messages
		WHERE sent_at IS NULL
		  AND scheduled_at <= $1
		ORDER BY scheduled_at ASC
		LIMIT $2
	`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("%s: query outbox: %w", op, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			r.log.ErrorContext(ctx, "rows close failed", slog.String("op", op), slog.Any("error", closeErr))
		}
	}()

	var outboxMessages []*models.OutboxMessage
	for rows.Next() {
		msg := &models.OutboxMessage{}
		var payload []byte
		if err := rows.Scan(
			&msg.OutboxID,
			&msg.BookingID,
			&msg.Topic,
			&msg.Key,
			&payload,
			&msg.ScheduledAt,
			&msg.SentAt,
			&msg.Attempts,
			&msg.LastError,
		); err != nil {
			return nil, fmt.Errorf("%s: scan outbox: %w", op, err)
		}
		msg.Payload = payload
		outboxMessages = append(outboxMessages, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: rows: %w", op, err)
	}

	return outboxMessages, nil
}

func (r *Repository) MarkOutboxSent(ctx context.Context, outboxID string) error {
	const op = "Repository.MarkOutboxSent"

	ctx, span := r.tracer.Start(ctx, op)
	defer span.End()

	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox_messages
		SET sent_at = now()
		WHERE outbox_id = $1::uuid
	`, outboxID)
	if err != nil {
		return fmt.Errorf("%s: update outbox: %w", op, err)
	}

	return nil
}

func (r *Repository) RescheduleOutbox(ctx context.Context, outboxID string, nextAttempt time.Time, lastErr string) error {
	const op = "Repository.RescheduleOutbox"

	ctx, span := r.tracer.Start(ctx, op)
	defer span.End()

	_, err := r.db.ExecContext(ctx, `
		UPDATE outbox_messages
		SET attempts = attempts + 1,
			last_error = $2,
			scheduled_at = $3
		WHERE outbox_id = $1::uuid
	`, outboxID, nullableString(lastErr), nextAttempt)
	if err != nil {
		return fmt.Errorf("%s: update outbox: %w", op, err)
	}

	return nil
}

func (r *Repository) GetBooking(ctx context.Context, bookingID string) (*models.Booking, error) {
	const op = "Repository.GetBooking"

	ctx, span := r.tracer.Start(ctx, op)
	defer span.End()

	booking, err := r.getBookingByID(ctx, bookingID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%s: %w", op, ErrBookingNotFound)
		}

		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return booking, nil
}

func (r *Repository) CancelBooking(ctx context.Context, bookingID string) (*models.Booking, error) {
	const op = "Repository.CancelBooking"

	ctx, span := r.tracer.Start(ctx, op)
	defer span.End()

	return r.cancelBooking(ctx, bookingID, "booking canceled", op)
}

func (r *Repository) ListBookingsByUser(ctx context.Context, userID string) ([]*models.Booking, error) {
	const op = "Repository.ListBookingsByUser"

	ctx, span := r.tracer.Start(ctx, op)
	defer span.End()

	rows, err := r.db.QueryContext(ctx, `
		SELECT
			booking_id::text,
			resource_id::text,
			user_id::text,
			resource_name,
			resource_location,
			resource_type,
			starts_at,
			ends_at,
			status::text,
			COALESCE(cancel_reason, ''),
			created_at,
			updated_at
		FROM bookings
		WHERE user_id = $1::uuid
		ORDER BY starts_at DESC
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("%s: query bookings: %w", op, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			r.log.ErrorContext(ctx, "rows close failed", slog.String("op", op), slog.Any("error", closeErr))
		}
	}()

	bookings, err := scanBookingRows(rows)
	if err != nil {
		return nil, fmt.Errorf("%s: scan bookings: %w", op, err)
	}

	return bookings, nil
}

func (r *Repository) ListBookingsByResource(ctx context.Context, resourceID string, from time.Time, to time.Time) ([]*models.Booking, error) {
	const op = "Repository.ListBookingsByResource"

	ctx, span := r.tracer.Start(ctx, op)
	defer span.End()

	rows, err := r.db.QueryContext(ctx, `
		SELECT
			booking_id::text,
			resource_id::text,
			user_id::text,
			resource_name,
			resource_location,
			resource_type,
			starts_at,
			ends_at,
			status::text,
			COALESCE(cancel_reason, ''),
			created_at,
			updated_at
		FROM bookings
		WHERE resource_id = $1::uuid
		  AND starts_at < $3
		  AND ends_at > $2
		ORDER BY starts_at ASC
	`, resourceID, from, to)
	if err != nil {
		return nil, fmt.Errorf("%s: query bookings: %w", op, err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			r.log.ErrorContext(ctx, "rows close failed", slog.String("op", op), slog.Any("error", closeErr))
		}
	}()

	bookings, err := scanBookingRows(rows)
	if err != nil {
		return nil, fmt.Errorf("%s: scan bookings: %w", op, err)
	}

	return bookings, nil
}

func (r *Repository) HasBookingConflict(ctx context.Context, userID string, startsAt time.Time, endsAt time.Time, gap time.Duration) (bool, error) {
	const op = "Repository.HasBookingConflict"

	ctx, span := r.tracer.Start(ctx, op)
	defer span.End()

	checkStart := startsAt.Add(-gap)
	checkEnd := endsAt.Add(gap)

	var hasConflict bool
	err := r.db.QueryRowContext(ctx, `
		SELECT EXISTS(
			SELECT 1
			FROM bookings
			WHERE user_id = $1::uuid
			  AND status = $2::booking_status
			  AND resource_type IN ($3, $4)
			  AND starts_at < $6
			  AND ends_at > $5
		)
	`, userID, dbStatusConfirmed, resourceTypeMeetingRoom, resourceTypeWorkspace, checkStart, checkEnd).Scan(&hasConflict)
	if err != nil {
		return false, fmt.Errorf("%s: check conflict: %w", op, err)
	}

	return hasConflict, nil
}

func (r *Repository) cancelBooking(ctx context.Context, bookingID string, cancelReason string, op string) (*models.Booking, error) {
	status := dbStatusCanceled
	updated := &models.Booking{}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: begin tx: %w", op, err)
	}

	err = tx.QueryRowContext(ctx, `
		UPDATE bookings
		SET status = $2::booking_status,
			cancel_reason = $3,
			updated_at = now()
		WHERE booking_id = $1::uuid
		  AND status = $4::booking_status
		RETURNING
			booking_id::text,
			resource_id::text,
			user_id::text,
			resource_name,
			resource_location,
			resource_type,
			starts_at,
			ends_at,
			status::text,
			COALESCE(cancel_reason, ''),
			created_at,
			updated_at
	`, bookingID, dbStatusCanceled, cancelReason, dbStatusConfirmed).Scan(
		&updated.BookingID,
		&updated.ResourceID,
		&updated.UserID,
		&updated.ResourceName,
		&updated.ResourceLocation,
		&updated.ResourceType,
		&updated.StartsAt,
		&updated.EndsAt,
		&status,
		&updated.CancelReason,
		&updated.CreatedAt,
		&updated.UpdatedAt,
	)
	if err == nil {
		if _, delErr := tx.ExecContext(ctx, `
			DELETE FROM outbox_messages
			WHERE booking_id = $1::uuid
			  AND sent_at IS NULL
		`, bookingID); delErr != nil {
			r.rollbackTransaction(ctx, tx, op)
			return nil, fmt.Errorf("%s: delete outbox: %w", op, delErr)
		}

		if err := tx.Commit(); err != nil {
			r.rollbackTransaction(ctx, tx, op)
			return nil, fmt.Errorf("%s: commit tx: %w", op, err)
		}

		updated.Status = dbStatusToDomain(status)
		return updated, nil
	}

	r.rollbackTransaction(ctx, tx, op)

	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%s: update booking: %w", op, err)
	}

	_, getErr := r.getBookingByID(ctx, bookingID)
	if errors.Is(getErr, sql.ErrNoRows) {
		return nil, fmt.Errorf("%s: %w", op, ErrBookingNotFound)
	}
	if getErr != nil {
		return nil, fmt.Errorf("%s: verify booking after cancel: %w", op, getErr)
	}

	return nil, fmt.Errorf("%s: booking already canceled", op)
}

func (r *Repository) getBookingByID(ctx context.Context, bookingID string) (*models.Booking, error) {
	booking := &models.Booking{}
	var status string

	err := r.db.QueryRowContext(ctx, `
		SELECT
			booking_id::text,
			resource_id::text,
			user_id::text,
			resource_name,
			resource_location,
			resource_type,
			starts_at,
			ends_at,
			status::text,
			COALESCE(cancel_reason, ''),
			created_at,
			updated_at
		FROM bookings
		WHERE booking_id = $1::uuid
	`, bookingID).Scan(
		&booking.BookingID,
		&booking.ResourceID,
		&booking.UserID,
		&booking.ResourceName,
		&booking.ResourceLocation,
		&booking.ResourceType,
		&booking.StartsAt,
		&booking.EndsAt,
		&status,
		&booking.CancelReason,
		&booking.CreatedAt,
		&booking.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}

	booking.Status = dbStatusToDomain(status)
	return booking, nil
}

func scanBookingRows(rows *sql.Rows) ([]*models.Booking, error) {
	bookings := make([]*models.Booking, 0)
	for rows.Next() {
		booking := &models.Booking{}
		var status string
		if err := rows.Scan(
			&booking.BookingID,
			&booking.ResourceID,
			&booking.UserID,
			&booking.ResourceName,
			&booking.ResourceLocation,
			&booking.ResourceType,
			&booking.StartsAt,
			&booking.EndsAt,
			&status,
			&booking.CancelReason,
			&booking.CreatedAt,
			&booking.UpdatedAt,
		); err != nil {
			return nil, err
		}

		booking.Status = dbStatusToDomain(status)
		bookings = append(bookings, booking)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return bookings, nil
}

func dbStatusToDomain(status string) models.BookingStatus {
	switch strings.ToLower(status) {
	case dbStatusConfirmed:
		return models.BookingStatusConfirmed
	case dbStatusCanceled:
		return models.BookingStatusCanceled
	default:
		return models.BookingStatus(strings.ToUpper(status))
	}
}

func domainStatusToDB(status models.BookingStatus) string {
	switch status {
	case models.BookingStatusConfirmed:
		return dbStatusConfirmed
	case models.BookingStatusCanceled:
		return dbStatusCanceled
	default:
		return ""
	}
}

func nullableString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}

	return v
}
