package metricsapp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type App struct {
	server *http.Server
	log    *slog.Logger
}

func New(port int, log *slog.Logger) *App {
	if log == nil {
		log = slog.Default()
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	return &App{
		server: &http.Server{
			Addr:              fmt.Sprintf(":%d", port),
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
		log: log.With(slog.String("component", "metrics_http")),
	}
}

func (a *App) Run() error {
	if a == nil || a.server == nil {
		return errors.New("metricsapp.Run: app is not initialized")
	}

	a.log.Info("metrics http server starting", slog.String("addr", a.server.Addr))
	err := a.server.ListenAndServe()
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		a.log.Info("metrics http server stopped")
		return nil
	}

	return err
}

func (a *App) Stop(ctx context.Context) error {
	if a == nil || a.server == nil {
		return nil
	}

	a.log.InfoContext(ctx, "stopping metrics http server")
	return a.server.Shutdown(ctx)
}
