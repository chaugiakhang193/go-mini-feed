COMPOSE := docker compose -f deployments/docker-compose.yml

.PHONY: help up down ps logs config fmt vet test race

help: ## List available targets
	@grep -E '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*## "} {printf "  %-8s %s\n", $$1, $$2}'

up: ## Start RabbitMQ and Redis, wait until healthy
	$(COMPOSE) up -d --wait

down: ## Stop and remove the containers
	$(COMPOSE) down

ps: ## Show container status
	$(COMPOSE) ps

logs: ## Follow container logs
	$(COMPOSE) logs -f

config: ## Load and print the gateway configuration
	go run ./cmd/feed-gateway

fmt: ## Format all Go code
	go fmt ./...

vet: ## Run go vet
	go vet ./...

test: ## Run unit tests
	go test ./...

race: ## Run unit tests with the race detector
	go test -race ./...
