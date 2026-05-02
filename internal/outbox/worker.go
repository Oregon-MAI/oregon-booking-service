package outbox

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/Oregon-MAI/oregon-booking-service/internal/domain/models"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const (
	defaultPollInterval = time.Minute
	defaultBatchSize    = 50
	defaultRetryBackoff = time.Minute
)

type Repository interface {
	ListDueOutbox(ctx context.Context, now time.Time, limit int) ([]*models.OutboxMessage, error)
	MarkOutboxSent(ctx context.Context, outboxID string) error
	RescheduleOutbox(ctx context.Context, outboxID string, nextAttempt time.Time, lastErr string) error
}

type Producer interface {
	ProduceEvent(ctx context.Context, topic string, key string, msg any) error
}

type Worker struct {
	repo         Repository
	producer     Producer
	log          *slog.Logger
	tracer       trace.Tracer
	pollInterval time.Duration
	batchSize    int
	retryBackoff time.Duration
}

func NewWorker(repo Repository, producer Producer, log *slog.Logger) *Worker {
	if log == nil {
		log = slog.Default()
	}

	return &Worker{
		repo:         repo,
		producer:     producer,
		log:          log,
		tracer:       otel.GetTracerProvider().Tracer("booking/outbox"),
		pollInterval: defaultPollInterval,
		batchSize:    defaultBatchSize,
		retryBackoff: defaultRetryBackoff,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	const op = "Outbox.Worker.Run"

	if w == nil || w.repo == nil || w.producer == nil {
		return errors.New("outbox worker is not initialized")
	}

	ctx, span := w.tracer.Start(ctx, op)
	defer span.End()

	ticker := time.NewTicker(w.pollInterval)
	defer ticker.Stop()

	w.log.InfoContext(ctx, "outbox worker started")
	defer w.log.InfoContext(ctx, "outbox worker stopped")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			w.processBatch(ctx)
		}
	}
}

func (w *Worker) processBatch(ctx context.Context) {
	const op = "Outbox.Worker.processBatch"

	ctx, span := w.tracer.Start(ctx, op)
	defer span.End()

	messages, err := w.repo.ListDueOutbox(ctx, time.Now().UTC(), w.batchSize)
	if err != nil {
		w.log.ErrorContext(ctx, "outbox fetch failed", slog.Any("error", err))
		return
	}

	for _, msg := range messages {
		if msg == nil {
			continue
		}

		err := w.producer.ProduceEvent(ctx, msg.Topic, msg.Key, msg.Payload)
		if err != nil {
			nextAttempt := time.Now().UTC().Add(w.retryBackoff)
			if updateErr := w.repo.RescheduleOutbox(ctx, msg.OutboxID, nextAttempt, err.Error()); updateErr != nil {
				w.log.ErrorContext(ctx, "outbox reschedule failed", slog.Any("error", updateErr))
			}
			w.log.WarnContext(ctx, "outbox publish failed", slog.String("outbox_id", msg.OutboxID), slog.Any("error", err))
			continue
		}

		if err := w.repo.MarkOutboxSent(ctx, msg.OutboxID); err != nil {
			w.log.ErrorContext(ctx, "outbox mark sent failed", slog.String("outbox_id", msg.OutboxID), slog.Any("error", err))
			continue
		}
	}
}
