MIGRATE=~/go/bin/migrate
SQLC=~/go/bin/sqlc
DB_URL=postgres://conveyor:conveyor@localhost:5432/conveyor?sslmode=disable

.PHONY: up down migrate-up migrate-down sqlc build run-api run-worker tidy test test-integration test-all test-cover docker-build

up:
	docker compose up -d

down:
	docker compose down

migrate-up:
	$(MIGRATE) -path internal/db/migrations -database "$(DB_URL)" -verbose up

migrate-down:
	$(MIGRATE) -path internal/db/migrations -database "$(DB_URL)" -verbose down

sqlc:
	cd internal/db && $(SQLC) generate

build:
	go build -o bin/api ./cmd/api
	go build -o bin/worker ./cmd/worker

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker

tidy:
	go mod tidy

test:
	go test ./...

test-integration:
	go test -tags integration -count=1 -timeout 60s -p 1 ./...

test-all: test test-integration

test-cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html

docker-build:
	docker build --build-arg BINARY=api    -t conveyor-api:latest    .
	docker build --build-arg BINARY=worker -t conveyor-worker:latest .
