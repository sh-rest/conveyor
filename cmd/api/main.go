package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sh-rest/conveyor/internal/api"
	"github.com/sh-rest/conveyor/internal/api/handler"
	"github.com/sh-rest/conveyor/internal/config"
	"github.com/sh-rest/conveyor/internal/db"
	"github.com/sh-rest/conveyor/internal/models"
	"github.com/sh-rest/conveyor/internal/queue"
)

func main() {
	// Structured JSON logging — every log line is a JSON object in prod
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Connect to Postgres
	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("failed to connect to postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	slog.Info("connected to postgres")

	q := models.New(pool)

	// Connect to Redis
	redisOpt, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		slog.Error("invalid redis url", "err", err)
		os.Exit(1)
	}
	rdb := redis.NewClient(redisOpt)
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Error("failed to connect to redis", "err", err)
		os.Exit(1)
	}
	slog.Info("connected to redis")

	producer := queue.NewProducer(rdb)

	// Wire handlers
	handlers := api.Handlers{
		Project:  handler.NewProjectHandler(q, cfg.APIKeyPrefix),
		Endpoint: handler.NewEndpointHandler(q),
		Event:    handler.NewEventHandler(q, producer),
		Delivery: handler.NewDeliveryHandler(q, producer),
		Metrics:  handler.NewMetricsHandler(q),
	}

	router := api.NewRouter(handlers, q)

	srv := &http.Server{
		Addr:         ":" + cfg.Port,
		Handler:      router,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// Start server in a goroutine so it doesn't block the shutdown signal listener
	go func() {
		slog.Info("api server starting", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	// Block until SIGTERM or SIGINT
	<-ctx.Done()
	slog.Info("shutting down...")

	// Give in-flight requests 15 seconds to finish
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("forced shutdown", "err", err)
	}

	slog.Info("server stopped")
}
