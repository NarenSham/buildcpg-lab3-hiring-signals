#!/usr/bin/env python3
"""Initialize DuckDB database with schema."""

import duckdb
import os

# Create warehouse directory
os.makedirs('warehouse', exist_ok=True)

# Connect to database
conn = duckdb.connect('warehouse/hiring_signals.duckdb')

# Read schema
with open('warehouse/schema.sql', 'r') as f:
    schema = f.read()

# Execute each statement
statements = [s.strip() for s in schema.split(';') if s.strip()]
for statement in statements:
    conn.execute(statement)

# Verify tables created
tables = conn.execute('SHOW TABLES').fetchall()
print(f'✅ Created {len(tables)} tables:', [t[0] for t in tables])

# Show schema for cleaned_jobs
print('\n📋 cleaned_jobs schema:')
schema = conn.execute('DESCRIBE cleaned_jobs').fetchall()
for column in schema:
    print(f'  - {column[0]}: {column[1]}')

conn.close()
print('\n✅ Database initialized successfully!')
