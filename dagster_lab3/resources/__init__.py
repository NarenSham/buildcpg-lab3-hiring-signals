"""Dagster resources for Lab 3."""

from dagster import ConfigurableResource
import duckdb
from pathlib import Path


class DuckDBResource(ConfigurableResource):
    """DuckDB database resource."""

    db_path: str = "warehouse/hiring_signals.duckdb"

    def get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get DuckDB connection.

        Returns:
            DuckDB connection object
        """
        db_file = Path(self.db_path)

        if not db_file.parent.exists():
            db_file.parent.mkdir(parents=True, exist_ok=True)

        conn = duckdb.connect(str(db_file))
        
        # Ensure tables exist (idempotent)
        self._ensure_schema(conn)
        
        return conn
    
    def _ensure_schema(self, conn: duckdb.DuckDBPyConnection) -> None:
        """Ensure all required tables exist."""
        
        # Check if raw_jobs exists
        result = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_name = 'raw_jobs'
        """).fetchone()
        
        if result[0] == 0:
            # Tables don't exist, create them
            schema_file = Path(__file__).parent.parent.parent / "warehouse" / "schema.sql"
            
            if schema_file.exists():
                with open(schema_file) as f:
                    schema_sql = f.read()
                
                # Execute each statement separately
                statements = [s.strip() for s in schema_sql.split(';') if s.strip()]
                for statement in statements:
                    try:
                        conn.execute(statement)
                    except Exception:
                        pass  # Table might already exist
            else:
                # Fallback: create minimal schema
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS raw_jobs (
                        job_id VARCHAR PRIMARY KEY,
                        company VARCHAR NOT NULL,
                        title VARCHAR NOT NULL,
                        description TEXT,
                        location VARCHAR,
                        posting_date DATE,
                        url VARCHAR,
                        source VARCHAR NOT NULL,
                        scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """)
