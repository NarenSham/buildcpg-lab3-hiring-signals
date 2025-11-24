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
    
    Handles both new jobs and updates to existing jobs:
    - New jobs: Insert with first_scraped_at = last_scraped_at = now
    - Existing jobs: Update only last_scraped_at to track how long job is open

    Returns:
        Number of NEW jobs inserted (not counting updates)
    """
    # Scrape jobs
    context.log.info("Starting Indeed scrape for Toronto jobs")
    jobs = scrape_toronto_jobs(limit=100)

    context.log.info(f"Scraped {len(jobs)} jobs from Indeed")

    # Load to DuckDB
    conn = duckdb.get_connection()

    # Check which job_ids already exist in database
    job_ids = [job["job_id"] for job in jobs]
    
    if len(job_ids) == 0:
        context.log.warning("No jobs scraped!")
        return Output(
            value=0,
            metadata={
                "total_jobs_scraped": 0,
                "jobs_inserted": 0,
                "existing_jobs_updated": 0,
            }
        )
    
    placeholders = ",".join("?" * len(job_ids))
    existing_jobs = conn.execute(
        f"SELECT job_id FROM raw_jobs WHERE job_id IN ({placeholders})",
        job_ids,
    ).fetchall()
    existing_job_ids = {row[0] for row in existing_jobs}

    context.log.info(f"Found {len(existing_job_ids)} existing jobs in database")

    # Separate new jobs from existing jobs
    new_jobs = [job for job in jobs if job["job_id"] not in existing_job_ids]
    existing_jobs_data = [job for job in jobs if job["job_id"] in existing_job_ids]

    # ========================================================================
    # INSERT NEW JOBS
    # ========================================================================
    if new_jobs:
        new_rows = [
            (
                job["job_id"],
                job["company"],
                job["title"],
                job.get("description"),
                job.get("location"),
                job.get("posting_date"),
                job.get("url"),
                job["source"],
                job["scraped_at"],  # first_scraped_at
                job["scraped_at"],  # last_scraped_at (initially same)
            )
            for job in new_jobs
        ]
        
        conn.executemany(
            """
            INSERT INTO raw_jobs (
                job_id, company, title, description, location,
                posting_date, url, source, first_scraped_at, last_scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            new_rows,
        )
        context.log.info(f"✅ Inserted {len(new_jobs)} NEW jobs")
    else:
        context.log.info("No new jobs to insert")

    # ========================================================================
    # UPDATE EXISTING JOBS (update last_scraped_at timestamp)
    # ========================================================================
    if existing_jobs_data:
        update_rows = [
            (job["scraped_at"], job["job_id"])
            for job in existing_jobs_data
        ]
        
        conn.executemany(
            """
            UPDATE raw_jobs 
            SET last_scraped_at = ?
            WHERE job_id = ?
            """,
            update_rows,
        )
        context.log.info(f"🔄 Updated {len(existing_jobs_data)} existing jobs (still open)")
    else:
        context.log.info("No existing jobs to update")

    inserted = len(new_jobs)
    existing_count = len(existing_jobs_data)

    context.log.info(
        f"Summary: {inserted} new jobs inserted, {existing_count} existing jobs updated"
    )

    # Add metadata
    return Output(
        value=inserted,
        metadata={
            "total_jobs_scraped": len(jobs),
            "jobs_inserted": inserted,
            "existing_jobs_updated": existing_count,
            "source": "indeed_rss",
            "location": "Toronto",
        },
    )