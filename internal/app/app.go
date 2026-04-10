package app

import (
	"context"
	"fmt"
	"log/slog"

	grpcapp "github.com/Oregon-MAI/oregon-booking-service/internal/app/grpc"
	"github.com/Oregon-MAI/oregon-booking-service/internal/config"
	resourcegrpc "github.com/Oregon-MAI/oregon-booking-service/internal/grpc/resource"
	repository "github.com/Oregon-MAI/oregon-booking-service/internal/repository/postgres"
	"github.com/Oregon-MAI/oregon-booking-service/internal/service"
)

type App struct {
	GRPC           *grpcapp.App
	repo           *repository.Repository
	resourceClient *resourcegrpc.Client
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
		_ = repo.Close()
		return nil, fmt.Errorf("%s: init resource client: %w", op, err)
	}

	bookingService := service.NewService(log, repo, resourceClient)
	grpcServer := grpcapp.New(cfg.GRPC.Port, bookingService, log)

	return &App{
		GRPC:           grpcServer,
		repo:           repo,
		resourceClient: resourceClient,
		log:            log,
	}, nil
}

func (a *App) MustRun() {
	a.GRPC.MustRun()
}

func (a *App) Run() error {
	if a == nil || a.GRPC == nil {
		return fmt.Errorf("app.Run: app is not initialized")
	}

	a.log.Info("starting grpc app")
	return a.GRPC.Run()
}

func (a *App) Stop() error {
	if a == nil {
		return nil
	}

	a.log.Info("stopping grpc app")
	if a.GRPC != nil {
		a.GRPC.Stop()
	}

	if a.resourceClient != nil {
		if err := a.resourceClient.Close(); err != nil {
			return fmt.Errorf("app.Stop: close resource client: %w", err)
		}
	}
	if a.repo != nil {
		if err := a.repo.Close(); err != nil {
			return fmt.Errorf("app.Stop: close repository: %w", err)
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

