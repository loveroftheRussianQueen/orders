.PHONY: up down run build lint test create-order list-orders get-order pay-order cancel-order dlq-trigger

## Infra
up:
	docker compose up -d

down:
	docker compose down -v

## App
run:
	go run ./cmd/server

build:
	go build -o bin/orders ./cmd/server

## Code
lint:
	go vet ./...

test:
	go test ./... -race -count=1

## API helpers (requires running service)
create-order:
	curl -s -X POST http://localhost:8080/api/v1/orders \
		-H 'Content-Type: application/json' \
		-d '{"user_id":1,"amount":149.99}' | jq .

list-orders:
	curl -s http://localhost:8080/api/v1/orders | jq .

get-order:
	curl -s http://localhost:8080/api/v1/orders/$(id) | jq .

pay-order:
	curl -s -X PATCH http://localhost:8080/api/v1/orders/$(id)/status \
		-H 'Content-Type: application/json' \
		-d '{"status":"paid"}' | jq .

cancel-order:
	curl -s -X PATCH http://localhost:8080/api/v1/orders/$(id)/status \
		-H 'Content-Type: application/json' \
		-d '{"status":"cancelled"}' | jq .

## Trigger DLQ: consumer will fail 3x and route to orders.dlq
dlq-trigger:
	curl -s -X POST http://localhost:8080/api/v1/orders \
		-H 'Content-Type: application/json' \
		-d '{"user_id":99,"amount":9999.99}' | jq .
