"""Cleaned jobs asset - deduplication and normalization."""

from dagster import asset, AssetExecutionContext, Output, MetadataValue, AssetCheckResult, asset_check
from dagster_lab3.resources import DuckDBResource
import polars as pl
from datetime import datetime


@asset(
    deps=["raw_jobs_asset"],  # Depends on raw_jobs
    description="Deduplicated and normalized job postings",
    group_name="transformation",
)
def cleaned_jobs_asset(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> Output[int]:
    """
    Transform raw jobs into cleaned, deduplicated dataset.
    
    Transformations:
    1. Deduplicate on (company, title, location)
    2. Normalize company names (lowercase, trim)
    3. Normalize job titles
    4. Track first_seen and last_seen dates
    
    Returns:
        Number of unique jobs after deduplication
    """
    conn = duckdb.get_connection()
    
    context.log.info("Reading raw jobs data")
    
    # Read raw_jobs using Polars (much faster than pandas)
    df = pl.read_database(
        query="SELECT * FROM raw_jobs",
        connection=conn
    )
    
    initial_count = len(df)
    context.log.info(f"Loaded {initial_count} raw jobs")
    
    # Step 1: Create deduplication key
    df = df.with_columns([
        # Normalize company name
        pl.col("company").str.to_lowercase().str.strip_chars().alias("company_normalized"),
        
        # Normalize title
        pl.col("title").str.to_lowercase().str.strip_chars().alias("title_normalized"),
    ])
    
    # Step 2: Create composite key for deduplication
    df = df.with_columns([
        pl.concat_str([
            pl.col("company_normalized"),
            pl.col("title_normalized"),
            pl.col("location"),
        ], separator="|").alias("dedup_key")
    ])
    
    # Step 3: Deduplicate - keep first occurrence, track dates
    df_deduped = (
        df
        .sort("scraped_at")  # Oldest first
        .group_by("dedup_key")
        .agg([
            pl.col("job_id").first().alias("job_id"),
            pl.col("company").first().alias("company"),
            pl.col("company_normalized").first(),
            pl.col("title").first().alias("title"),
            pl.col("title_normalized").first(),
            pl.col("description").first().alias("description"),
            pl.col("location").first().alias("location"),
            pl.col("posting_date").first().alias("posting_date"),
            pl.col("url").first().alias("url"),
            pl.col("source").first().alias("source"),
            pl.col("scraped_at").min().alias("first_seen"),
            pl.col("scraped_at").max().alias("last_seen"),
        ])
    )
    
    cleaned_count = len(df_deduped)
    duplicates_removed = initial_count - cleaned_count
    
    context.log.info(
        f"Deduplication complete: {cleaned_count} unique jobs "
        f"({duplicates_removed} duplicates removed)"
    )
    
    # Step 4: Write to cleaned_jobs table
    # First, ensure table exists
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cleaned_jobs (
            job_id VARCHAR PRIMARY KEY,
            company VARCHAR NOT NULL,
            company_normalized VARCHAR NOT NULL,
            title VARCHAR NOT NULL,
            title_normalized VARCHAR NOT NULL,
            description TEXT,
            location VARCHAR,
            posting_date DATE,
            url VARCHAR,
            source VARCHAR,
            first_seen TIMESTAMP,
            last_seen TIMESTAMP
        )
    """)
    
    # Write data
    conn.execute("DELETE FROM cleaned_jobs")  # Replace all data
    conn.execute("""
        INSERT INTO cleaned_jobs 
        SELECT * FROM df_deduped
    """)
    
    context.log.info(f"Wrote {cleaned_count} cleaned jobs to database")
    
    # Step 5: Gather metadata for observability
    
    # Company distribution
    company_stats = (
        df_deduped
        .group_by("company_normalized")
        .agg([pl.count().alias("job_count")])
        .sort("job_count", descending=True)
        .head(10)
    )
    
    top_companies = {
        row["company_normalized"]: row["job_count"] 
        for row in company_stats.to_dicts()
    }
    
    # Source distribution
    source_stats = (
        df_deduped
        .group_by("source")
        .agg([pl.count().alias("count")])
    )
    
    source_breakdown = {
        row["source"]: row["count"]
        for row in source_stats.to_dicts()
    }
    
    # Date range
    date_range = df_deduped.select([
        pl.col("posting_date").min().alias("earliest"),
        pl.col("posting_date").max().alias("latest"),
    ]).to_dicts()[0]
    
    # Return with rich metadata
    return Output(
        value=cleaned_count,
        metadata={
            "raw_job_count": initial_count,
            "cleaned_job_count": cleaned_count,
            "duplicates_removed": duplicates_removed,
            "deduplication_rate": f"{(duplicates_removed / initial_count * 100):.1f}%",
            "date_range": f"{date_range['earliest']} to {date_range['latest']}",
            "top_companies": MetadataValue.json(top_companies),
            "source_breakdown": MetadataValue.json(source_breakdown),
        },
    )


@asset_check(asset=cleaned_jobs_asset, description="Ensure no null companies")
def check_no_null_companies(duckdb: DuckDBResource) -> AssetCheckResult:
    """Data quality check: no null company names."""
    conn = duckdb.get_connection()
    
    result = conn.execute("""
        SELECT COUNT(*) as null_count
        FROM cleaned_jobs
        WHERE company_normalized IS NULL OR company_normalized = ''
    """).fetchone()
    
    null_count = result[0]
    total_count = conn.execute("SELECT COUNT(*) FROM cleaned_jobs").fetchone()[0]
    
    passed = null_count == 0
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "null_count": null_count,
            "total_count": total_count,
            "null_percentage": f"{(null_count / max(total_count, 1) * 100):.2f}%",
        },
        severity="ERROR" if not passed else None,
    )


@asset_check(asset=cleaned_jobs_asset, description="Ensure reasonable job count")
def check_reasonable_job_count(duckdb: DuckDBResource) -> AssetCheckResult:
    """Data quality check: we should have at least 50 jobs."""
    conn = duckdb.get_connection()
    
    count = conn.execute("SELECT COUNT(*) FROM cleaned_jobs").fetchone()[0]
    
    min_expected = 50
    passed = count >= min_expected
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "job_count": count,
            "min_expected": min_expected,
        },
        severity="WARN" if not passed else None,
    )


@asset_check(asset=cleaned_jobs_asset, description="Ensure deduplication worked")
def check_deduplication_effectiveness(duckdb: DuckDBResource) -> AssetCheckResult:
    """Data quality check: verify no duplicate jobs."""
    conn = duckdb.get_connection()
    
    # Check for duplicates by normalized company + title
    duplicates = conn.execute("""
        SELECT 
            company_normalized,
            title_normalized,
            COUNT(*) as duplicate_count
        FROM cleaned_jobs
        GROUP BY company_normalized, title_normalized
        HAVING COUNT(*) > 1
    """).fetchall()
    
    duplicate_count = len(duplicates)
    passed = duplicate_count == 0
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "duplicate_groups": duplicate_count,
            "examples": str(duplicates[:3]) if duplicates else "None",
        },
        severity="ERROR" if not passed else None,
    )
