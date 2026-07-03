package kafka

import (
	"context"
	"log/slog"
	"time"

	"orders/internal/metrics"
	"orders/internal/repository"
)

type OutboxWorker struct {
	repo     *repository.Repository
	producer *Producer
	interval time.Duration
}

func NewOutboxWorker(repo *repository.Repository, producer *Producer) *OutboxWorker {
	return &OutboxWorker{
		repo:     repo,
		producer: producer,
		interval: time.Second,
	}
}

func (w *OutboxWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.processBatch(ctx)
		}
	}
}

func (w *OutboxWorker) processBatch(ctx context.Context) {
	rows, err := w.repo.GetPendingOutbox(ctx, 10)
	if err != nil {
		slog.Error("outbox: fetch failed", "err", err)
		return
	}

	count, _ := w.repo.CountPendingOutbox(ctx)
	metrics.OutboxPending.Set(float64(count))

	for _, row := range rows {
		w.processRow(ctx, row.ID, row.Topic, row.Key, row.Payload)
	}
}

func (w *OutboxWorker) processRow(ctx context.Context, id int64, topic, key string, payload []byte) {
	defer func() {
		if r := recover(); r != nil {
			slog.Error("outbox: panic, sending to DLQ", "err", r, "row_id", id)
			_ = w.producer.Write(ctx, topic+".dlq", key, payload)
			_ = w.repo.MarkOutboxSent(ctx, id)
		}
	}()

	if err := w.producer.Write(ctx, topic, key, payload); err != nil {
		slog.Error("outbox: kafka produce failed", "err", err, "row_id", id)
		return
	}

	if err := w.repo.MarkOutboxSent(ctx, id); err != nil {
		slog.Error("outbox: mark sent failed", "err", err, "row_id", id)
	}

	slog.Info("outbox: sent", "row_id", id, "topic", topic, "key", key)
}
