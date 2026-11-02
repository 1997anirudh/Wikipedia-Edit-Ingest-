.PHONY: up down logs test lint

up: ## Build and start the full pipeline
	docker compose up --build -d

down: ## Stop the pipeline (data volume is kept)
	docker compose down

logs: ## Tail logs from all services
	docker compose logs -f

test: ## Run the unit test suite (no live services needed)
	python -m pytest tests/ -q

lint: ## Lint the codebase
	ruff check .
