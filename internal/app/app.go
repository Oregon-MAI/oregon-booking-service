package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	grpcapp "github.com/Oregon-MAI/oregon-booking-service/internal/app/grpc"
	kafkaproducer "github.com/Oregon-MAI/oregon-booking-service/internal/brokers/kafka"
	"github.com/Oregon-MAI/oregon-booking-service/internal/config"
	resourcegrpc "github.com/Oregon-MAI/oregon-booking-service/internal/grpc/resource"
	"github.com/Oregon-MAI/oregon-booking-service/internal/outbox"
	repository "github.com/Oregon-MAI/oregon-booking-service/internal/repository/postgres"
	"github.com/Oregon-MAI/oregon-booking-service/internal/service"
)

type App struct {
	GRPC           *grpcapp.App
	repo           *repository.Repository
	resourceClient *resourcegrpc.Client
	producer       *kafkaproducer.Producer
	outboxWorker   *outbox.Worker
	outboxCancel   context.CancelFunc
	outboxDone     chan struct{}
	log            *slog.Logger
}

func New(ctx context.Context, cfg *config.Config, log *slog.Logger) (*App, error) {
	const op = "App.New"

	if cfg == nil {
		return nil, fmt.Errorf("%s: config is nil", op)
	}
	if log == nil {
		log = slog.Default()
	}

	dsn := makeDSN(cfg.Database)

	repo, err := repository.New(ctx, dsn, log)
	if err != nil {
		return nil, fmt.Errorf("%s: init repository: %w", op, err)
	}

	resourceClient, err := resourcegrpc.NewClient(cfg.ResourceService.Address)
	if err != nil {
		if closeErr := repo.Close(); closeErr != nil {
			log.ErrorContext(ctx, "failed to close repository", slog.Any("error", closeErr))
		}
		return nil, fmt.Errorf("%s: init resource client: %w", op, err)
	}

	producer, err := initProducer(ctx, cfg, log, repo, resourceClient, op)
	if err != nil {
		return nil, err
	}

	bookingService := service.NewService(log, repo, resourceClient, producer, service.EventTopics{
		UserBooking: cfg.Kafka.Topics.UserBooking,
		AdminCancel: cfg.Kafka.Topics.AdminCancel,
		UserCancel:  cfg.Kafka.Topics.UserCancel,
		RemindStart: cfg.Kafka.Topics.RemindStart,
		RemindEnd:   cfg.Kafka.Topics.RemindEnd,
	})
	grpcServer := grpcapp.New(cfg.GRPC.Port, bookingService, log)

	var outboxWorker *outbox.Worker
	if cfg.Kafka.Enabled && producer != nil {
		outboxWorker = outbox.NewWorker(repo, producer, log)
	}

	return &App{
		GRPC:           grpcServer,
		repo:           repo,
		resourceClient: resourceClient,
		producer:       producer,
		outboxWorker:   outboxWorker,
		outboxDone:     make(chan struct{}),
		log:            log,
	}, nil
}

func initProducer(
	ctx context.Context,
	cfg *config.Config,
	log *slog.Logger,
	repo *repository.Repository,
	resourceClient *resourcegrpc.Client,
	op string,
) (*kafkaproducer.Producer, error) {
	if !cfg.Kafka.Enabled {
		return nil, nil
	}

	producer, err := kafkaproducer.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.ClientID, log)
	if err != nil {
		if closeErr := resourceClient.Close(); closeErr != nil {
			log.ErrorContext(ctx, "failed to close resource client", slog.Any("error", closeErr))
		}
		if closeErr := repo.Close(); closeErr != nil {
			log.ErrorContext(ctx, "failed to close repository", slog.Any("error", closeErr))
		}
		return nil, fmt.Errorf("%s: init kafka producer: %w", op, err)
	}

	log.InfoContext(ctx, "kafka producer initialized", slog.Any("brokers", cfg.Kafka.Brokers))
	return producer, nil
}

func (a *App) MustRun() {
	a.GRPC.MustRun()
}

func (a *App) Run() error {
	if a == nil || a.GRPC == nil {
		return errors.New("app.Run: app is not initialized")
	}

	if a.outboxWorker != nil {
		ctx, cancel := context.WithCancel(context.Background())
		a.outboxCancel = cancel
		go func() {
			defer close(a.outboxDone)
			if err := a.outboxWorker.Run(ctx); err != nil {
				a.log.WarnContext(ctx, "outbox worker stopped", slog.Any("error", err))
			}
		}()
	}

	a.log.InfoContext(context.Background(), "starting grpc app")
	return a.GRPC.Run()
}

func (a *App) Stop(ctx context.Context) error {
	const op = "App.Stop"

	if a == nil {
		return nil
	}

	a.log.InfoContext(ctx, "stopping grpc app")
	if a.GRPC != nil {
		a.GRPC.Stop(ctx)
	}

	if a.resourceClient != nil {
		if err := a.resourceClient.Close(); err != nil {
			return fmt.Errorf("%s: close resource client: %w", op, err)
		}
	}
	if a.outboxCancel != nil {
		a.outboxCancel()
		<-a.outboxDone
	}
	if a.producer != nil {
		if err := a.producer.Close(); err != nil {
			a.log.ErrorContext(ctx, "app.Stop: close producer failed", slog.Any("error", err))
		} else {
			a.log.InfoContext(ctx, "kafka producer closed")
		}
	}
	if a.repo != nil {
		if err := a.repo.Close(); err != nil {
			return fmt.Errorf("%s: close repository: %w", op, err)
		}
	}

	a.log.InfoContext(ctx, "application stopped")

	return nil
}

func makeDSN(cfg config.Database) string {
	return fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.Host,
		cfg.Port,
		cfg.User,
		cfg.Password,
		cfg.Name,
		cfg.SSLMode,
	)
}
