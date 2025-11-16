"""DuckDB data loader."""

from pathlib import Path
from typing import Any

import duckdb
import structlog

logger = structlog.get_logger()


class DuckDBLoader:
    """Load data into DuckDB warehouse."""

    def __init__(self, db_path: str = "warehouse/hiring_signals.duckdb"):
        """Initialize loader.

        Args:
            db_path: Path to DuckDB database file
        """
        self.db_path = Path(db_path)

        if not self.db_path.exists():
            logger.warning("database_not_found", path=str(self.db_path))
            logger.info("creating_database", path=str(self.db_path))

        self.conn = duckdb.connect(str(self.db_path))

    def load_jobs(self, jobs: list[dict[str, Any]]) -> int:
        """Load jobs into raw_jobs table.

        Args:
            jobs: List of job dictionaries

        Returns:
            Number of jobs inserted (excluding duplicates)
        """
        if not jobs:
            logger.warning("no_jobs_to_load")
            return 0

        logger.info("loading_jobs", job_count=len(jobs))

        # Prepare data for insertion
        rows = [
            (
                job["job_id"],
                job["company"],
                job["title"],
                job.get("description"),
                job.get("location"),
                job.get("posting_date"),
                job.get("url"),
                job["source"],
                job["scraped_at"],
            )
            for job in jobs
        ]

        # Insert with ON CONFLICT DO NOTHING (deduplication)
        result = self.conn.executemany(
            """
            INSERT INTO raw_jobs (
                job_id, company, title, description, location,
                posting_date, url, source, scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (job_id) DO NOTHING
            """,
            rows,
        )

        # Count how many were actually inserted
        inserted = len(jobs) - self._count_duplicates(jobs)

        logger.info(
            "jobs_loaded",
            total_jobs=len(jobs),
            inserted=inserted,
            duplicates=len(jobs) - inserted,
        )

        return inserted

    def _count_duplicates(self, jobs: list[dict[str, Any]]) -> int:
        """Count how many jobs already exist in database."""
        job_ids = [job["job_id"] for job in jobs]

        placeholders = ",".join("?" * len(job_ids))
        query = f"""
            SELECT COUNT(*)
            FROM raw_jobs
            WHERE job_id IN ({placeholders})
        """

        result = self.conn.execute(query, job_ids).fetchone()
        return result[0] if result else 0

    def get_job_count(self, days: int = 7) -> int:
        """Get count of jobs scraped in last N days.

        Args:
            days: Number of days to look back

        Returns:
            Job count
        """
        result = self.conn.execute(
            """
            SELECT COUNT(*)
            FROM raw_jobs
            WHERE scraped_at >= CURRENT_TIMESTAMP - INTERVAL ? DAY
            """,
            [days],
        ).fetchone()

        return result[0] if result else 0

    def close(self):
        """Close database connection."""
        self.conn.close()


def load_jobs_to_duckdb(
    jobs: list[dict[str, Any]], db_path: str = "warehouse/hiring_signals.duckdb"
) -> int:
    """Convenience function to load jobs to DuckDB.

    Args:
        jobs: List of job dictionaries
        db_path: Path to DuckDB database

    Returns:
        Number of jobs inserted
    """
    loader = DuckDBLoader(db_path)
    inserted = loader.load_jobs(jobs)
    loader.close()
    return inserted


if __name__ == "__main__":
    # Test the loader
    import structlog

    structlog.configure(processors=[structlog.processors.JSONRenderer()])

    from scraper import scrape_toronto_jobs

    # Scrape jobs
    jobs = scrape_toronto_jobs(limit=20)

    # Load to DuckDB
    inserted = load_jobs_to_duckdb(jobs)

    print(f"\n✅ Loaded {inserted} new jobs to DuckDB")
