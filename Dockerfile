FROM python:3.11-slim

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code
COPY dagster_lab3 ./dagster_lab3
COPY src ./src
COPY warehouse/schema.sql ./warehouse/schema.sql
COPY scripts/init_db.py ./scripts/init_db.py
COPY config ./config

# Expose port
EXPOSE 3000

# Startup script
CMD ["sh", "-c", "\
    mkdir -p /tmp/dagster_home && \
    export DAGSTER_HOME=/tmp/dagster_home && \
    python scripts/init_db.py && \
    dagster dev -h 0.0.0.0 -p 3000 -m dagster_lab3.definitions\
"]