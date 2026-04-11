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
	repository "github.com/Oregon-MAI/oregon-booking-service/internal/repository/postgres"
	"github.com/Oregon-MAI/oregon-booking-service/internal/service"
)

type App struct {
	GRPC           *grpcapp.App
	repo           *repository.Repository
	resourceClient *resourcegrpc.Client
	producer       *kafkaproducer.Producer
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
			return nil, fmt.Errorf("%s: init resource client: %w; close repository: %v", op, err, closeErr)
		}

		return nil, fmt.Errorf("%s: init resource client: %w", op, err)
	}

	var producer *kafkaproducer.Producer
	if cfg.Kafka.Enabled {
		producer, err = kafkaproducer.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.ClientID, log)
		if err != nil {
			if closeResourceErr := resourceClient.Close(); closeResourceErr != nil {
				if closeRepositoryErr := repo.Close(); closeRepositoryErr != nil {
					return nil, fmt.Errorf("%s: init kafka producer: %w; close resource client: %v; close repository: %v", op, err, closeResourceErr, closeRepositoryErr)
				}

				return nil, fmt.Errorf("%s: init kafka producer: %w; close resource client: %v", op, err, closeResourceErr)
			}

			if closeRepositoryErr := repo.Close(); closeRepositoryErr != nil {
				return nil, fmt.Errorf("%s: init kafka producer: %w; close repository: %v", op, err, closeRepositoryErr)
			}

			return nil, fmt.Errorf("%s: init kafka producer: %w", op, err)
		}

		log.Info("kafka producer initialized", slog.Any("brokers", cfg.Kafka.Brokers))
	}

	bookingService := service.NewService(log, repo, resourceClient, producer, service.EventTopics{
		UserBooking:    cfg.Kafka.Topics.UserBooking,
		AdminCancel: cfg.Kafka.Topics.AdminCancel,
		UserCancel:  cfg.Kafka.Topics.UserCancel,
	})
	grpcServer := grpcapp.New(cfg.GRPC.Port, bookingService, log)

	return &App{
		GRPC:           grpcServer,
		repo:           repo,
		resourceClient: resourceClient,
		producer:       producer,
		log:            log,
	}, nil
}

func (a *App) MustRun() {
	a.GRPC.MustRun()
}

func (a *App) Run() error {
	if a == nil || a.GRPC == nil {
		return errors.New("app.Run: app is not initialized")
	}

	a.log.Info("starting grpc app")
	return a.GRPC.Run()
}

func (a *App) Stop() error {
	const op = "App.Stop"

	if a == nil {
		return nil
	}

	a.log.Info("stopping grpc app")
	if a.GRPC != nil {
		a.GRPC.Stop()
	}

	if a.resourceClient != nil {
		if err := a.resourceClient.Close(); err != nil {
			return fmt.Errorf("%s: close resource client: %w", op, err)
		}
	}
	if a.producer != nil {
		if err := a.producer.Close(); err != nil {
			return fmt.Errorf("%s: close kafka producer: %w", op, err)
		}
	}
	if a.repo != nil {
		if err := a.repo.Close(); err != nil {
			return fmt.Errorf("%s: close repository: %w", op, err)
		}
	}

	a.log.Info("application stopped")

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
