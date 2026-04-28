package grpcapp

import (
	"context"
	"fmt"
	"log/slog"
	"net"

	bookinghandler "github.com/Oregon-MAI/oregon-booking-service/internal/grpc/booking"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

type App struct {
	port     int
	server   *grpc.Server
	listener net.Listener
	log      *slog.Logger
}

func New(port int, bookingService bookinghandler.BookingService, log *slog.Logger) *App {
	if log == nil {
		log = slog.Default()
	}

	server := grpc.NewServer(
		grpc.StatsHandler(otelgrpc.NewServerHandler()),
		grpc.ChainUnaryInterceptor(
			rpcLoggingUnaryInterceptor(log),
			recoveryUnaryInterceptor(log),
		),
	)

	reflection.Register(server)
	bookinghandler.NewServer(server, bookingService)

	return &App{
		port:   port,
		server: server,
		log:    log,
	}
}

func (a *App) MustRun() {
	if err := a.Run(); err != nil {
		panic(err)
	}
}

func (a *App) Run() error {
	const op = "grpcapp.Run"

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", a.port))
	if err != nil {
		return fmt.Errorf("%s: listen: %w", op, err)
	}
	a.listener = listener

	a.log.InfoContext(context.Background(), "grpc listener started", slog.String("addr", listener.Addr().String()))

	err = a.server.Serve(listener)
	if err == nil || err == grpc.ErrServerStopped {
		a.log.InfoContext(context.Background(), "grpc server stopped")
		return nil
	}

	return fmt.Errorf("%s: serve: %w", op, err)
}

func (a *App) Stop(ctx context.Context) {
	if a == nil || a.server == nil {
		return
	}

	a.log.InfoContext(ctx, "stopping grpc server")
	a.server.GracefulStop()
}

func recoveryUnaryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (resp any, err error) {
		defer func() {
			if recovered := recover(); recovered != nil {
				log.ErrorContext(ctx, "panic recovered in grpc handler", slog.String("method", info.FullMethod), slog.Any("panic", recovered))
				err = status.Error(codes.Internal, "internal error")
			}
		}()

		return handler(ctx, req)
	}
}

func rpcLoggingUnaryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req any,
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			log.WarnContext(ctx,
				"grpc request failed",
				slog.String("method", info.FullMethod),
				slog.String("grpc_code", status.Code(err).String()),
				slog.Any("error", err),
			)
		}

		return resp, err
	}
}
