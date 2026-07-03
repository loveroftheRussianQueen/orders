package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"orders/internal/model"
	"orders/internal/repository"
)

var (
	ErrOrderLocked       = errors.New("another order from this user is being processed")
	ErrInvalidTransition = errors.New("order is already in a terminal state")
)

type OrderService struct {
	repo  *repository.Repository
	redis *redis.Client
}

func NewOrderService(repo *repository.Repository, rdb *redis.Client) *OrderService {
	return &OrderService{repo: repo, redis: rdb}
}

// CreateOrder acquires a per-user Redis lock, then creates the order + outbox row atomically.
func (s *OrderService) CreateOrder(ctx context.Context, req model.CreateOrderRequest) (model.Order, error) {
	lockKey := fmt.Sprintf("order_lock:%d", req.UserID)

	ok, err := s.redis.SetNX(ctx, lockKey, 1, 5*time.Second).Result()
	if err != nil {
		return model.Order{}, fmt.Errorf("redis lock: %w", err)
	}
	if !ok {
		return model.Order{}, ErrOrderLocked
	}
	defer s.redis.Del(ctx, lockKey)

	return s.repo.CreateOrderWithOutbox(ctx, req)
}

// GetOrder returns from Redis cache when available, falls back to DB.
func (s *OrderService) GetOrder(ctx context.Context, id int64) (model.Order, error) {
	cacheKey := fmt.Sprintf("order:%d", id)

	// try cache
	var order model.Order
	err := s.redis.Get(ctx, cacheKey).Scan(&order)
	if err == nil {
		return order, nil
	}
	if !errors.Is(err, redis.Nil) {
		slog.Warn("redis get order", "err", err)
	}

	// cache miss
	order, err = s.repo.GetOrder(ctx, id)
	if err != nil {
		return model.Order{}, err
	}

	// populate cache; ignore errors
	s.redis.Set(ctx, cacheKey, order, 5*time.Minute)
	return order, nil
}

func (s *OrderService) ListOrders(ctx context.Context, f model.ListFilter) ([]model.Order, error) {
	return s.repo.ListOrders(ctx, f)
}

// UpdateStatus updates order status + writes outbox row, then invalidates cache.
func (s *OrderService) UpdateStatus(ctx context.Context, id int64, status model.Status) (model.Order, error) {
	current, err := s.GetOrder(ctx, id)
	if err != nil {
		return model.Order{}, err
	}
	if current.Status == model.StatusPaid || current.Status == model.StatusCancelled {
		return model.Order{}, ErrInvalidTransition
	}

	order, err := s.repo.UpdateStatusWithOutbox(ctx, id, status)
	if err != nil {
		return model.Order{}, err
	}

	s.redis.Del(ctx, fmt.Sprintf("order:%d", id))
	return order, nil
}
