package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"

	"orders/internal/metrics"
	"orders/internal/model"
	"orders/internal/repository"
)

type Consumer struct {
	reader    *kafka.Reader
	dlqWriter *kafka.Writer
	repo      *repository.Repository
}

func NewConsumer(broker, topic, groupID string, repo *repository.Repository) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  []string{broker},
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 1,
			MaxBytes: 10e6,
		}),
		dlqWriter: &kafka.Writer{
			Addr:     kafka.TCP(broker),
			Topic:    topic + ".dlq",
			Balancer: &kafka.LeastBytes{},
		},
		repo: repo,
	}
}

func (c *Consumer) Run(ctx context.Context) {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("consumer: fetch failed", "err", err)
			continue
		}

		c.handleWithRetry(ctx, msg)

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			slog.Error("consumer: commit failed", "err", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

const maxRetries = 3

func (c *Consumer) handleWithRetry(ctx context.Context, msg kafka.Message) {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = c.handle(ctx, msg)
		if err == nil {
			metrics.KafkaEventsProcessed.WithLabelValues("success").Inc()
			return
		}
		slog.Warn("consumer: retry", "attempt", i+1, "err", err)
		time.Sleep(time.Duration(i+1) * time.Second)
	}

	slog.Error("consumer: max retries exceeded, sending to DLQ", "key", string(msg.Key))
	if err := c.dlqWriter.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
	}); err != nil {
		slog.Error("consumer: DLQ write failed", "err", err)
	}
	metrics.KafkaEventsProcessed.WithLabelValues("dlq").Inc()
}

func (c *Consumer) handle(ctx context.Context, msg kafka.Message) error {
	var event model.OrderEvent
	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	// idempotency check
	exists, err := c.repo.EventExists(ctx, event.EventID)
	if err != nil {
		return fmt.Errorf("idempotency check: %w", err)
	}
	if exists {
		slog.Info("consumer: duplicate event, skipping", "event_id", event.EventID)
		return nil
	}

	// Trigger DLQ testing: create an order with amount 9999.99
	if event.Amount == 9999.99 {
		return fmt.Errorf("simulated error (amount=9999.99 triggers DLQ)")
	}

	slog.Info("consumer: processed event",
		"event_id", event.EventID,
		"event_type", event.EventType,
		"order_id", event.OrderID,
		"status", event.Status,
		"amount", event.Amount,
	)

	return c.repo.MarkEventProcessed(ctx, event.EventID)
}
