.DEFAULT_GOAL := help

PROJECT_NAME := o11yhn

-include .env
export

.PHONY: help
help:
	@echo "------------------------------------------------------------------------"
	@echo "${PROJECT_NAME}"
	@echo "------------------------------------------------------------------------"
	@grep -h -E '^[a-zA-Z0-9_/%\-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: fmt
fmt: ## Run go fmt on the codebase
	go fmt ./...

.PHONY: vet
vet: ## Run go vet on the codebase
	go vet ./...

.PHONY: producer
producer: ## Run the producer
	go run ./cmd/producer/main.go

.PHONY: consumer
consumer: ## Run the consumer
	go run ./cmd/consumer/main.go

.PHONY: db-down
db-down: ## Stop the local database
	docker compose down

.PHONY: db-up
db-up: ## Start the local database
	docker compose up -d

.PHONY: ch
ch: ## Open ClickHouse client shell
	docker exec -it clickhouse clickhouse-client --user "$(CLICKHOUSE_USER)" --password "$(CLICKHOUSE_PASSWORD)" --database default

.PHONY: db-clean
db-clean: ## Full reset local stack and volumes (destructive)
	docker compose down -v && docker compose up -d

.PHONY: urls
urls: ## Print local service URLs
	@echo "Redpanda Console: http://localhost:8080"
	@echo "ClickHouse HTTP:  http://localhost:8123"
	@echo "Kafka Broker:     localhost:9092"
