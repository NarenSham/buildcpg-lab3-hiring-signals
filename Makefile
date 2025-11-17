.PHONY: help setup clean test lint format dagster-dev dagster-materialize snapshot db-init db-query check-python

help:
	@echo "Lab 3: Hiring Signals Pipeline"
	@echo ""
	@echo "Available commands:"
	@echo "  make check-python       - Verify Python version"
	@echo "  make setup              - Initialize database and install deps"
	@echo "  make db-init            - Initialize/reset database schema"
	@echo "  make db-query           - Query database stats"
	@echo "  make dagster-dev        - Start Dagster UI (localhost:3000)"
	@echo "  make dagster-materialize - Materialize all assets"
	@echo "  make test               - Run pytest"
	@echo "  make lint               - Run ruff linter"
	@echo "  make format             - Format code with ruff"
	@echo "  make snapshot           - Export weekly snapshot to Parquet"
	@echo "  make clean              - Remove generated files"

check-python:
	@python --version 2>&1 | grep -q "Python 3\." || \
		(echo "❌ Error: Python 3.10+ required. Current version:" && python --version && \
		 echo "\nPlease activate a Python 3 virtual environment:" && \
		 echo "  python3 -m venv venv" && \
		 echo "  source venv/bin/activate" && \
		 exit 1)
	@echo "✅ Python version OK: $$(python --version)"

setup: check-python db-init
	pip install -r requirements.txt
	@echo "✅ Setup complete. Run 'make dagster-dev' to start."

db-init: check-python
	@echo "🔧 Initializing DuckDB schema..."
	@mkdir -p warehouse
	@python -c "\
import duckdb; \
conn = duckdb.connect('warehouse/hiring_signals.duckdb'); \
schema = open('warehouse/schema.sql').read(); \
statements = [s.strip() for s in schema.split(';') if s.strip()]; \
[conn.execute(s) for s in statements]; \
tables = conn.execute('SHOW TABLES').fetchall(); \
print('✅ Tables created:', [t[0] for t in tables]); \
conn.close()"

db-query: check-python
	@python -c "\
import duckdb; \
conn = duckdb.connect('warehouse/hiring_signals.duckdb'); \
count = conn.execute('SELECT COUNT(*) FROM raw_jobs').fetchone()[0]; \
print(f'📊 Total jobs: {count}'); \
companies = conn.execute('SELECT company, COUNT(*) as cnt FROM raw_jobs GROUP BY company ORDER BY cnt DESC LIMIT 5').fetchall(); \
print('🏢 Top companies:'); \
[print(f'  - {c}: {n}') for c, n in companies]; \
conn.close()"

dagster-dev: check-python
	dagster dev -m dagster_lab3.definitions

dagster-materialize: check-python
	dagster asset materialize --select '*' -m dagster_lab3.definitions

test: check-python
	pytest tests/ -v --cov=src --cov=dagster_lab3

lint:
	ruff check src/ dagster_lab3/ tests/

format:
	ruff format src/ dagster_lab3/ tests/

snapshot: check-python
	@echo "Exporting snapshot to Parquet..."
	@mkdir -p warehouse/snapshots
	@python -c "\
import duckdb; \
from datetime import datetime; \
conn = duckdb.connect('warehouse/hiring_signals.duckdb'); \
week = datetime.now().strftime('%Y-W%U'); \
conn.execute(f\"EXPORT DATABASE 'warehouse/snapshots/{week}' (FORMAT PARQUET)\"); \
print(f\"✅ Snapshot saved to warehouse/snapshots/{week}/\")"

clean:
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf dagster_lab3/tmpg*/
	rm -rf .dagster/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
