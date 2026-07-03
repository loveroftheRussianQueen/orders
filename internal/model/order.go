package model

import "time"

type Status string

const (
	StatusPending   Status = "pending"
	StatusPaid      Status = "paid"
	StatusCancelled Status = "cancelled"
)

type Order struct {
	ID        int64     `json:"id"`
	UserID    int64     `json:"user_id"`
	Amount    float64   `json:"amount"`
	Status    Status    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
}

type CreateOrderRequest struct {
	UserID int64   `json:"user_id"`
	Amount float64 `json:"amount"`
}

type UpdateStatusRequest struct {
	Status Status `json:"status"`
}

// OrderEvent is the Kafka message payload written by the outbox worker.
type OrderEvent struct {
	EventID   string    `json:"event_id"`
	EventType string    `json:"event_type"` // "order.created" | "order.status_changed"
	OrderID   int64     `json:"order_id"`
	UserID    int64     `json:"user_id"`
	Amount    float64   `json:"amount"`
	Status    string    `json:"status"`
	Timestamp time.Time `json:"timestamp"`
}

type ListFilter struct {
	Status Status
	Limit  int
	Offset int
}

// OutboxRow is a row from the outbox table.
type OutboxRow struct {
	ID      int64
	Topic   string
	Key     string
	Payload []byte
}
