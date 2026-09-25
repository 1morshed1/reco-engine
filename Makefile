# Developer entrypoints. Run `make help` for the list.
#
# Go may not be on PATH depending on how it was installed; we prepend the
# standard install location so these targets work out of the box.
export PATH := /usr/local/go/bin:$(PATH)

# Connection strings the commands read from the environment. Defaults match
# docker-compose.yml so a fresh clone works with zero config.
export POSTGRES_DSN ?= postgres://reco:reco@localhost:5432/reco?sslmode=disable
export REDIS_ADDR   ?= localhost:6379
export KAFKA_BROKERS ?= localhost:9092
export KAFKA_TOPIC   ?= user-events
export API_ADDR      ?= :8080

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

.PHONY: up
up: ## Start infra (postgres, redis, kafka, prometheus)
	docker compose up -d

.PHONY: down
down: ## Stop infra
	docker compose down

.PHONY: reset
reset: ## Stop infra AND wipe postgres volume (fresh DB)
	docker compose down -v

.PHONY: tidy
tidy: ## Resolve/verify Go module dependencies
	go mod tidy

.PHONY: build
build: ## Compile all binaries into ./bin
	go build -o bin/api       ./cmd/api
	go build -o bin/processor ./cmd/processor
	go build -o bin/seed      ./cmd/seed
	go build -o bin/producer  ./cmd/producer
	go build -o bin/eval      ./cmd/eval
	go build -o bin/batch     ./cmd/batch
	go build -o bin/loadtest  ./cmd/loadtest

.PHONY: migrate
migrate: ## Apply DB schema (idempotent)
	go run ./cmd/seed -migrate-only

.PHONY: seed
seed: ## Load catalog + generate item embeddings
	go run ./cmd/seed

.PHONY: api
api: ## Run the recommendation API
	go run ./cmd/api

.PHONY: processor
processor: ## Run the Kafka event processor
	go run ./cmd/processor

.PHONY: producer
producer: ## Simulate user events into Kafka
	go run ./cmd/producer

.PHONY: eval
eval: ## Run offline evaluation (precision@k, recall@k, nDCG)
	go run ./cmd/eval

.PHONY: batch
batch: ## Run the nightly batch job (embeddings, co-occurrence, trending rescale)
	go run ./cmd/batch

.PHONY: loadtest
loadtest: ## Benchmark the API (throughput + latency percentiles)
	go run ./cmd/loadtest -c 50 -d 12s -users 500

.PHONY: test
test: ## Run unit tests
	go test ./...
