COMPOSE := docker compose -f deployments/docker-compose.yml

.PHONY: help up down ps logs smoke config fmt vet test race

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

smoke: ## Check vhosts, the dev user and Redis in the running stack
	$(COMPOSE) exec -T rabbitmq rabbitmqctl -q list_vhosts
	$(COMPOSE) exec -T rabbitmq rabbitmqctl -q list_user_permissions lab
	$(COMPOSE) exec -T rabbitmq rabbitmqctl -q authenticate_user lab lab
	$(COMPOSE) exec -T redis redis-cli ping

config: ## Load and print the gateway configuration
	go run ./cmd/feed-gateway

fmt: ## Format all Go code
	go fmt ./...

vet: ## Run go vet
	go vet ./...

test: ## Run unit tests
	go test ./...

race: ## Run unit tests with the race detector (never cached)
	go test -race -count=1 ./...
