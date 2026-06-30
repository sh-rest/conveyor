package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/redis/go-redis/v9"
	"github.com/sh-rest/conveyor/internal/api/handler"
	"github.com/sh-rest/conveyor/internal/config"
	"github.com/sh-rest/conveyor/internal/db"
	"github.com/sh-rest/conveyor/internal/models"
	"github.com/sh-rest/conveyor/internal/queue"
	"github.com/sh-rest/conveyor/internal/ratelimit"
	"github.com/sh-rest/conveyor/internal/worker"
)

func main() {
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

	// Set up queue components
	consumer := queue.NewConsumer(rdb, "worker-1")
	if err := consumer.SetupGroup(ctx); err != nil {
		slog.Error("failed to setup consumer group", "err", err)
		os.Exit(1)
	}

	scheduler := queue.NewScheduler(rdb)
	limiter := ratelimit.NewLimiter(rdb)
	dispatcher := worker.NewDispatcher(q, scheduler, limiter)
	workerPool := worker.NewPool(dispatcher, consumer, scheduler, cfg.WorkerCount)

	// Render requires a bound port even for background workloads
	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		})
		mux.HandleFunc("/readyz", handler.NewHealthHandler(pool, rdb).Readyz)
		if err := http.ListenAndServe(":"+cfg.Port, mux); err != nil {
			slog.Error("health server error", "err", err)
		}
	}()

	slog.Info("worker starting", "concurrency", cfg.WorkerCount)
	workerPool.Start(ctx, cfg.WorkerCount)

	// Block until SIGTERM/SIGINT
	<-ctx.Done()
	slog.Info("shutting down — draining in-flight deliveries...")

	// Wait for all goroutines to finish their current work
	workerPool.Wait()
	slog.Info("worker stopped")
}
