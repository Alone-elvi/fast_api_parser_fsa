.PHONY: all build build-base up down restart logs clean migrate-postgres wait-postgres analyze-certs

# Переменные
DOCKER_COMPOSE = docker compose
BASE_IMAGE = fsa-base:latest
POSTGRES_CONTAINER = fast_api_parser_fsa-postgres-1
MAX_RETRIES = 60
SLEEP_TIME = 2

# Переменные для сервисов
PROCESSING_SERVICE = processing_service
INGESTION_SERVICE = ingestion_service
ANALYTICS_SERVICE = analytics_service

# Дефолтная цель
all: build-base build up wait-postgres migrate-postgres

# Собрать базовый образ
build-base:
	docker build -t $(BASE_IMAGE) -f app/base.Dockerfile .

# Собрать все сервисы
build: build-base
	$(DOCKER_COMPOSE) build

# Запустить все сервисы
up:
	$(DOCKER_COMPOSE) up -d

# Ждем готовности PostgreSQL
wait-postgres:
	@echo "Waiting for PostgreSQL to be ready..."
	@for i in $$(seq 1 $(MAX_RETRIES)); do \
		if docker exec $(POSTGRES_CONTAINER) pg_isready -U postgres > /dev/null 2>&1; then \
			echo "PostgreSQL is ready!"; \
			exit 0; \
		fi; \
		echo "Waiting... $$i/$(MAX_RETRIES)"; \
		sleep $(SLEEP_TIME); \
	done; \
	echo "Error: PostgreSQL did not become ready in time"; \
	exit 1

# Применить миграции PostgreSQL
migrate-postgres:
	@echo "Applying PostgreSQL migrations..."
	@if docker exec $(POSTGRES_CONTAINER) psql -U postgres -d fsa_parser_db -c "SELECT 1" > /dev/null 2>&1; then \
		echo "Database is accessible, migrations already applied during initialization"; \
	else \
		echo "Error: Could not connect to PostgreSQL database"; \
		exit 1; \
	fi

# Остановить все сервисы
down:
	$(DOCKER_COMPOSE) down

# Перезапустить все сервисы
restart: down up

# Показать логи
logs:
	$(DOCKER_COMPOSE) logs -f

# Показать логи конкретного сервиса
logs-service:
	$(DOCKER_COMPOSE) logs -f $(service)

# Очистить все
clean: down
	docker system prune -f
	docker volume prune -f

# Пересоздать и запустить все
rebuild: clean all

# Проверить статус сервисов
ps:
	$(DOCKER_COMPOSE) ps

# Пересобрать и перезапустить processing_service
rebuild-processing: 
	$(DOCKER_COMPOSE) stop $(PROCESSING_SERVICE)
	$(DOCKER_COMPOSE) rm -f $(PROCESSING_SERVICE)
	$(DOCKER_COMPOSE) build --no-cache $(PROCESSING_SERVICE)
	$(DOCKER_COMPOSE) up -d $(PROCESSING_SERVICE)
	$(DOCKER_COMPOSE) logs -f $(PROCESSING_SERVICE)

# Пересобрать и перезапустить ingestion_service
rebuild-ingestion:
	$(DOCKER_COMPOSE) stop $(INGESTION_SERVICE)
	$(DOCKER_COMPOSE) rm -f $(INGESTION_SERVICE)
	$(DOCKER_COMPOSE) build --no-cache $(INGESTION_SERVICE)
	$(DOCKER_COMPOSE) up -d $(INGESTION_SERVICE)
	$(DOCKER_COMPOSE) logs -f $(INGESTION_SERVICE)

# Пересобрать и перезапустить analytics_service
rebuild-analytics:
	$(DOCKER_COMPOSE) stop $(ANALYTICS_SERVICE)
	$(DOCKER_COMPOSE) rm -f $(ANALYTICS_SERVICE)
	$(DOCKER_COMPOSE) build --no-cache $(ANALYTICS_SERVICE)
	$(DOCKER_COMPOSE) up -d $(ANALYTICS_SERVICE)
	$(DOCKER_COMPOSE) logs -f $(ANALYTICS_SERVICE)

# Пересобрать конкретный сервис (использование: make rebuild-service service=имя_сервиса)
rebuild-service:
	@if [ "$(service)" = "" ]; then \
		echo "Error: Please specify service name (make rebuild-service service=name)"; \
		exit 1; \
	fi
	$(DOCKER_COMPOSE) stop $(service)
	$(DOCKER_COMPOSE) rm -f $(service)
	$(DOCKER_COMPOSE) build --no-cache $(service)
	$(DOCKER_COMPOSE) up -d $(service)
	$(DOCKER_COMPOSE) logs -f $(service)

# Обновить help
help:
	@echo "Available commands:"
	@echo "  make all          - Build base image, build all services, start them and apply migrations (default)"
	@echo "  make build-base   - Build base image"
	@echo "  make build        - Build all services"
	@echo "  make up           - Start all services"
	@echo "  make wait-postgres - Wait for PostgreSQL to be ready"
	@echo "  make migrate-postgres - Apply PostgreSQL migrations"
	@echo "  make down         - Stop all services"
	@echo "  make restart      - Restart all services"
	@echo "  make logs         - Show all logs"
	@echo "  make logs-service service=NAME - Show logs for specific service"
	@echo "  make clean        - Clean all containers and volumes"
	@echo "  make rebuild      - Rebuild and restart all services"
	@echo "  make ps           - Show services status"
	@echo "  make rebuild-processing - Rebuild and restart processing service"
	@echo "  make rebuild-ingestion - Rebuild and restart ingestion service"
	@echo "  make rebuild-analytics - Rebuild and restart analytics service"
	@echo "  make rebuild-service service=NAME - Rebuild and restart specific service"
	@echo "  make analyze-certs - Analyze certificates"

# Анализ сертификатов
analyze-certs:
	docker compose run --rm processing_service python -m processing_service.utils.analyze_certificates