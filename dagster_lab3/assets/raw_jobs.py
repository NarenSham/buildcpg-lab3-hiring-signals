"""Raw jobs asset - scraping from Indeed."""

from dagster import asset, AssetExecutionContext, Output
from dagster_lab3.resources import DuckDBResource
import sys
from pathlib import Path

# Add src to path so we can import scraper
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from scraper import scrape_toronto_jobs


@asset(
    description="Raw job postings scraped from Indeed Toronto RSS feed",
    group_name="ingestion",
)
def raw_jobs_asset(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> Output[int]:
    """Scrape Indeed RSS feed and load to DuckDB.

    Returns:
        Number of jobs inserted (excluding duplicates)
    """
    # Scrape jobs
    context.log.info("Starting Indeed scrape for Toronto jobs")
    jobs = scrape_toronto_jobs(limit=100)

    context.log.info(f"Scraped {len(jobs)} jobs from Indeed")

    # Load to DuckDB
    conn = duckdb.get_connection()

    # Prepare data
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

    # Insert with deduplication
    conn.executemany(
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
    job_ids = [job["job_id"] for job in jobs]
    placeholders = ",".join("?" * len(job_ids))

    existing_count = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM raw_jobs
        WHERE job_id IN ({placeholders})
        """,
        job_ids,
    ).fetchone()[0]

    inserted = len(jobs) - existing_count

    context.log.info(
        f"Loaded {inserted} new jobs (found {existing_count} duplicates)"
    )

    # Add metadata
    return Output(
        value=inserted,
        metadata={
            "total_jobs_scraped": len(jobs),
            "jobs_inserted": inserted,
            "duplicates_found": existing_count,
            "source": "indeed_rss",
            "location": "Toronto",
        },
    )
