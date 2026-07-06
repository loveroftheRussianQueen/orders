# orders

A Go service for creating and tracking orders.

Stack: Go, PostgreSQL, Kafka, Redis, Prometheus, Grafana.

## Running locally

```bash
docker compose up -d   # starts postgres, kafka, redis, prometheus, grafana
go run ./cmd/server
```

That's it. Migrations run automatically on first postgres start.

## API

```
POST   /api/v1/orders              create order
GET    /api/v1/orders              list orders (last 100)
GET    /api/v1/orders/:id          get order by id
PATCH  /api/v1/orders/:id/status   update status: paid | cancelled
GET    /health
GET    /metrics
```

Create example:

```bash
curl -X POST http://localhost:8080/api/v1/orders \
  -H 'Content-Type: application/json' \
  -d '{"user_id": 1, "amount": 149.99}'
```

## How it works

**Order creation** acquires a per-user Redis lock (SetNX, 5s TTL) to prevent duplicate concurrent orders from the same user. The order row and the outbox event are written in a single transaction — either both land or neither does.

**Outbox worker** polls the `outbox` table every second and produces pending messages to Kafka, then marks them sent. This means Kafka being temporarily down doesn't break order creation.

**Kafka consumer** reads from the `orders` topic, checks `processed_events` for duplicates, retries up to 3 times on failure, and routes unrecoverable messages to `orders.dlq`.

**Caching** — `GET /orders/:id` reads from Redis first (5 min TTL). Status updates invalidate the cache.

## Observability

Grafana at http://localhost:3000 (admin/admin) comes pre-provisioned with a dashboard.

Metrics exposed:
- `http_requests_total` — by method, path, status code
- `http_request_duration_seconds` — latency histogram
- `outbox_pending_total` — how many events are waiting to be sent
- `kafka_events_processed_total` — success vs dlq

## Config

All via env vars, defaults work out of the box for local docker compose:

```
HTTP_ADDR      :8080
DB_URL         postgres://postgres:postgres@localhost:5432/orders?sslmode=disable
KAFKA_BROKER   localhost:9092
REDIS_ADDR     localhost:6379
```

## DLQ testing

Create an order with `amount: 9999.99` — the consumer will intentionally fail 3 times and send it to `orders.dlq`.
