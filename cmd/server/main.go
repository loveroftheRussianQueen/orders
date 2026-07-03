package main

import (
	"context"
	"log/slog"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"

	"orders/internal/config"
	appkafka "orders/internal/kafka"
	"orders/internal/handler"
	"orders/internal/repository"
	"orders/internal/service"
)

func main() {
	cfg := config.Load()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- PostgreSQL ---
	pool := mustConnectDB(ctx, cfg.DBURL)
	defer pool.Close()

	// --- Redis ---
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		slog.Error("redis connect failed", "err", err)
	}

	// --- Kafka producer (shared by outbox worker) ---
	producer := appkafka.NewProducer(cfg.KafkaBroker)
	defer producer.Close()

	// --- Repository & service ---
	repo := repository.New(pool)
	svc := service.NewOrderService(repo, rdb)

	// --- Outbox worker ---
	outboxWorker := appkafka.NewOutboxWorker(repo, producer)
	go outboxWorker.Run(ctx)

	// --- Kafka consumer ---
	consumer := appkafka.NewConsumer(cfg.KafkaBroker, "orders", "orders-group", repo)
	defer consumer.Close()
	go consumer.Run(ctx)

	// --- HTTP router ---
	r := chi.NewRouter()
	r.Use(handler.MetricsMiddleware)

	orderHandler := handler.NewOrderHandler(svc)

	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"status":"ok"}`))
	})
	r.Handle("/metrics", promhttp.Handler())

	r.Route("/api/v1/orders", func(r chi.Router) {
		r.Post("/", orderHandler.Create)
		r.Get("/", orderHandler.List)
		r.Get("/{id}", orderHandler.Get)
		r.Patch("/{id}/status", orderHandler.UpdateStatus)
	})

	// --- HTTP server ---
	server := &http.Server{
		Addr:    cfg.HTTPAddr,
		Handler: r,
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutCtx)
	}()

	slog.Info("server starting", "addr", cfg.HTTPAddr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server error", "err", err)
	}

	slog.Info("server stopped")
}

func mustConnectDB(ctx context.Context, url string) *pgxpool.Pool {
	var pool *pgxpool.Pool
	var err error
	for i := 0; i < 10; i++ {
		pool, err = pgxpool.New(ctx, url)
		if err == nil {
			if err = pool.Ping(ctx); err == nil {
				slog.Info("postgres connected")
				return pool
			}
		}
		slog.Warn("postgres not ready, retrying...", "attempt", i+1)
		time.Sleep(2 * time.Second)
	}
	slog.Error("postgres connect failed", "err", err)
	panic(err)
}
