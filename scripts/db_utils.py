#!/usr/bin/env python3
"""Database utility commands."""

import duckdb
import sys
from pathlib import Path

DB_PATH = "warehouse/hiring_signals.duckdb"


def init_schema():
    """Initialize database schema."""
    conn = duckdb.connect(DB_PATH)
    
    schema_file = Path("warehouse/schema.sql")
    if not schema_file.exists():
        print("❌ Schema file not found: warehouse/schema.sql")
        sys.exit(1)
    
    with open(schema_file) as f:
        schema_sql = f.read()
    
    # Split into statements and execute each
    statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
    
    for statement in statements:
        try:
            conn.execute(statement)
        except Exception as e:
            print(f"⚠️  Error: {e}")
            print(f"Statement: {statement[:100]}...")
    
    tables = conn.execute("SHOW TABLES").fetchall()
    print(f"✅ Created {len(tables)} tables:")
    for table in tables:
        print(f"  - {table[0]}")
    
    conn.close()


def query_stats():
    """Query database statistics."""
    conn = duckdb.connect(DB_PATH)
    
    # Check if tables exist
    tables = conn.execute("SHOW TABLES").fetchall()
    if not tables:
        print("❌ No tables found. Run 'make db-init' first.")
        conn.close()
        return
    
    # Total jobs
    count = conn.execute("SELECT COUNT(*) FROM raw_jobs").fetchone()[0]
    print(f"📊 Total jobs: {count}")
    
    if count > 0:
        # Date range
        date_range = conn.execute("""
            SELECT 
                MIN(posting_date) as earliest,
                MAX(posting_date) as latest
            FROM raw_jobs
        """).fetchone()
        print(f"📅 Date range: {date_range[0]} to {date_range[1]}")
        
        # Top companies
        print("\n🏢 Top 10 companies:")
        companies = conn.execute("""
            SELECT company, COUNT(*) as job_count
            FROM raw_jobs
            GROUP BY company
            ORDER BY job_count DESC
            LIMIT 10
        """).fetchall()
        
        for company, job_count in companies:
            print(f"  - {company}: {job_count} job(s)")
        
        # Recent jobs
        print("\n📋 Recent jobs (last 5):")
        recent = conn.execute("""
            SELECT company, title, posting_date
            FROM raw_jobs
            ORDER BY scraped_at DESC
            LIMIT 5
        """).fetchall()
        
        for company, title, date in recent:
            print(f"  - {company}: {title} ({date})")
    
    conn.close()


def reset_db():
    """Reset database (drop all data)."""
    response = input("⚠️  This will delete all data. Continue? (yes/no): ")
    if response.lower() != "yes":
        print("Aborted.")
        return
    
    db_file = Path(DB_PATH)
    if db_file.exists():
        db_file.unlink()
        print("✅ Database deleted.")
        print("Run 'make db-init' to recreate schema.")
    else:
        print("ℹ️  Database file doesn't exist.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python scripts/db_utils.py init    - Initialize schema")
        print("  python scripts/db_utils.py stats   - Show database stats")
        print("  python scripts/db_utils.py reset   - Reset database")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "init":
        init_schema()
    elif command == "stats":
        query_stats()
    elif command == "reset":
        reset_db()
    else:
        print(f"❌ Unknown command: {command}")
        sys.exit(1)
