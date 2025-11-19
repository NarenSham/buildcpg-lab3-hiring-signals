.PHONY: help setup dev down logs clean test lint format db-query db-init rebuild

help:
	@echo "Lab 3: Hiring Signals Pipeline (Docker Mode)"
	@echo ""
	@echo "Available commands:"
	@echo "  make dev                - Start Dagster in Docker (localhost:3000)"
	@echo "  make down               - Stop Docker containers"
	@echo "  make logs               - View container logs"
	@echo "  make rebuild            - Rebuild and restart containers"
	@echo "  make db-init            - Reinitialize database in container"
	@echo "  make db-query           - Query database stats"
	@echo "  make shell              - Open shell in container"
	@echo "  make test               - Run tests in container"
	@echo "  make lint               - Run ruff linter"
	@echo "  make format             - Format code with ruff"
	@echo "  make clean              - Remove containers and volumes"

dev:
	docker-compose up -d
	@echo "✅ Dagster running at http://localhost:3000"

down:
	docker-compose down

logs:
	docker-compose logs -f dagster

rebuild:
	docker-compose down
	docker-compose build --no-cache
	docker-compose up -d
	@echo "✅ Rebuilt and restarted"

db-init:
	docker-compose exec dagster python scripts/init_db.py
	@echo "✅ Database reinitialized"

db-query:
	docker-compose exec dagster python -c "\
import duckdb; \
conn = duckdb.connect('warehouse/hiring_signals.duckdb'); \
count = conn.execute('SELECT COUNT(*) FROM raw_jobs').fetchone()[0]; \
print(f'📊 Total jobs: {count}'); \
companies = conn.execute('SELECT company, COUNT(*) as cnt FROM raw_jobs GROUP BY company ORDER BY cnt DESC LIMIT 5').fetchall(); \
print('🏢 Top companies:'); \
[print(f'  - {c}: {n}') for c, n in companies]; \
conn.close()"

shell:
	docker-compose exec dagster /bin/bash

test:
	docker-compose exec dagster pytest tests/ -v

lint:
	ruff check src/ dagster_lab3/ tests/

format:
	ruff format src/ dagster_lab3/ tests/

clean:
	docker-compose down -v
	rm -rf .pytest_cache/ htmlcov/ .coverage .dagster/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "✅ Cleaned up containers, volumes, and temp files"