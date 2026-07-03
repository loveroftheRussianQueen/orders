package repository

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"orders/internal/model"
)

type Repository struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

// CreateOrderWithOutbox inserts an order and an outbox row in a single transaction.
func (r *Repository) CreateOrderWithOutbox(ctx context.Context, req model.CreateOrderRequest) (model.Order, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return model.Order{}, err
	}
	defer tx.Rollback(ctx)

	var order model.Order
	err = tx.QueryRow(ctx,
		`INSERT INTO orders (user_id, amount, status)
		 VALUES ($1, $2, 'pending')
		 RETURNING id, user_id, amount, status, created_at`,
		req.UserID, req.Amount,
	).Scan(&order.ID, &order.UserID, &order.Amount, &order.Status, &order.CreatedAt)
	if err != nil {
		return model.Order{}, err
	}

	event := model.OrderEvent{
		EventID:   fmt.Sprintf("order-%d-created-%d", order.ID, time.Now().UnixNano()),
		EventType: "order.created",
		OrderID:   order.ID,
		UserID:    order.UserID,
		Amount:    order.Amount,
		Status:    string(order.Status),
		Timestamp: order.CreatedAt,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return model.Order{}, err
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`,
		"orders", fmt.Sprintf("%d", order.ID), payload,
	)
	if err != nil {
		return model.Order{}, err
	}

	return order, tx.Commit(ctx)
}

// UpdateStatusWithOutbox updates order status and writes an outbox row in one transaction.
func (r *Repository) UpdateStatusWithOutbox(ctx context.Context, id int64, status model.Status) (model.Order, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return model.Order{}, err
	}
	defer tx.Rollback(ctx)

	var order model.Order
	err = tx.QueryRow(ctx,
		`UPDATE orders SET status=$1 WHERE id=$2
		 RETURNING id, user_id, amount, status, created_at`,
		status, id,
	).Scan(&order.ID, &order.UserID, &order.Amount, &order.Status, &order.CreatedAt)
	if err != nil {
		return model.Order{}, err
	}

	event := model.OrderEvent{
		EventID:   fmt.Sprintf("order-%d-status-%s-%d", order.ID, status, time.Now().UnixNano()),
		EventType: "order.status_changed",
		OrderID:   order.ID,
		UserID:    order.UserID,
		Amount:    order.Amount,
		Status:    string(status),
		Timestamp: time.Now(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return model.Order{}, err
	}

	_, err = tx.Exec(ctx,
		`INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)`,
		"orders", fmt.Sprintf("%d", order.ID), payload,
	)
	if err != nil {
		return model.Order{}, err
	}

	return order, tx.Commit(ctx)
}

func (r *Repository) GetOrder(ctx context.Context, id int64) (model.Order, error) {
	var order model.Order
	err := r.pool.QueryRow(ctx,
		`SELECT id, user_id, amount, status, created_at FROM orders WHERE id=$1`, id,
	).Scan(&order.ID, &order.UserID, &order.Amount, &order.Status, &order.CreatedAt)
	return order, err
}

func (r *Repository) ListOrders(ctx context.Context, f model.ListFilter) ([]model.Order, error) {
	var (
		query string
		args  []any
	)
	if f.Status != "" {
		query = `SELECT id, user_id, amount, status, created_at FROM orders WHERE status=$1 ORDER BY id DESC LIMIT $2 OFFSET $3`
		args = []any{f.Status, f.Limit, f.Offset}
	} else {
		query = `SELECT id, user_id, amount, status, created_at FROM orders ORDER BY id DESC LIMIT $1 OFFSET $2`
		args = []any{f.Limit, f.Offset}
	}

	rows, err := r.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var orders []model.Order
	for rows.Next() {
		var o model.Order
		if err := rows.Scan(&o.ID, &o.UserID, &o.Amount, &o.Status, &o.CreatedAt); err != nil {
			return nil, err
		}
		orders = append(orders, o)
	}
	return orders, rows.Err()
}

// GetPendingOutbox returns up to limit unsent outbox rows.
func (r *Repository) GetPendingOutbox(ctx context.Context, limit int) ([]model.OutboxRow, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, topic, key, payload FROM outbox WHERE sent=false ORDER BY id LIMIT $1`, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []model.OutboxRow
	for rows.Next() {
		var row model.OutboxRow
		if err := rows.Scan(&row.ID, &row.Topic, &row.Key, &row.Payload); err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

func (r *Repository) MarkOutboxSent(ctx context.Context, id int64) error {
	_, err := r.pool.Exec(ctx, `UPDATE outbox SET sent=true WHERE id=$1`, id)
	return err
}

func (r *Repository) CountPendingOutbox(ctx context.Context) (int, error) {
	var count int
	err := r.pool.QueryRow(ctx, `SELECT COUNT(*) FROM outbox WHERE sent=false`).Scan(&count)
	return count, err
}

func (r *Repository) EventExists(ctx context.Context, eventID string) (bool, error) {
	var exists bool
	err := r.pool.QueryRow(ctx,
		`SELECT EXISTS(SELECT 1 FROM processed_events WHERE event_id=$1)`, eventID,
	).Scan(&exists)
	return exists, err
}

func (r *Repository) MarkEventProcessed(ctx context.Context, eventID string) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, eventID,
	)
	return err
}
