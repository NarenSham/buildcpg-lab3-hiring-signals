"""Company statistics asset - configuration-driven tech detection."""

from dagster import asset, AssetExecutionContext, Output, MetadataValue
from dagster_lab3.resources import DuckDBResource
import polars as pl


@asset(
    deps=["cleaned_jobs_asset"],
    description="Weekly company hiring statistics with config-driven tech detection",
    group_name="analytics",
)
def company_stats_asset(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> Output[int]:
    """
    Aggregate cleaned jobs into weekly company statistics.
    
    CONFIGURATION-DRIVEN APPROACH
    =============================
    
    ALL tech detection logic comes from the tech_config table!
    
    To add a new technology:
    ------------------------
    INSERT INTO tech_config VALUES 
        ('Go', ARRAY['golang', 'go'], 'language', true, 1.0);
    
    Then re-materialize this asset. That's it!
    
    How it works:
    -------------
    1. Load all techs from tech_config table
    2. CROSS JOIN with jobs (creates job × tech combinations)
    3. Check if ANY keyword matches the job
    4. Aggregate matches by company + week
    """
    conn = duckdb.get_connection()
    
    # Check tech_config is populated
    tech_count = conn.execute("SELECT COUNT(*) FROM tech_config").fetchone()[0]
    
    if tech_count == 0:
        context.log.error("❌ tech_config table is empty! No technologies to detect.")
        return Output(
            value=0,
            metadata={"error": "tech_config table is empty"}
        )
    
    context.log.info(f"📋 Loaded {tech_count} technologies from config")
    
    # List techs for visibility
    techs = conn.execute("SELECT tech_name FROM tech_config ORDER BY tech_name").fetchall()
    context.log.info(f"Tracking: {', '.join([t[0] for t in techs])}")
    
    # ============================================================================
    # CONFIG-DRIVEN TECH DETECTION QUERY
    # ============================================================================
    
    query = """
    -- Step 1: Weekly job grouping
    -- ============================
    WITH weekly_jobs AS (
        SELECT 
            company_normalized,
            company,
            date_trunc('week', posting_date) as week_start,
            title,
            description,
            location
        FROM cleaned_jobs
        WHERE posting_date IS NOT NULL
    ),

    -- Step 2: CROSS JOIN jobs with tech_config
    -- =========================================
    job_tech_pairs AS (
        SELECT 
            w.company_normalized,
            w.company,
            w.week_start,
            w.title,
            w.description,
            w.location,
            t.tech_name,
            t.keywords,
            
            -- Seniority detection
            CASE 
                WHEN LOWER(w.title) LIKE '%senior%' OR LOWER(w.title) LIKE '%sr.%'
                THEN 'Senior'
                WHEN LOWER(w.title) LIKE '%junior%' OR LOWER(w.title) LIKE '%jr.%'
                THEN 'Junior'
                WHEN LOWER(w.title) LIKE '%lead%' OR LOWER(w.title) LIKE '%principal%'
                THEN 'Lead'
                ELSE 'Mid-Level'
            END as seniority
        FROM weekly_jobs w
        CROSS JOIN tech_config t
    ),

    -- Step 3: Check if ANY keyword matches
    -- ====================================
    tech_matches AS (
        SELECT 
            company_normalized,
            company,
            week_start,
            title,
            location,
            seniority,
            tech_name,
            
            -- Check if ANY keyword appears
            (
                SELECT bool_or(
                    LOWER(title) LIKE '%' || keyword || '%' 
                    OR LOWER(COALESCE(description, '')) LIKE '%' || keyword || '%'
                )
                FROM unnest(keywords) AS t(keyword)
            ) as has_tech
            
        FROM job_tech_pairs
    ),

    -- Step 4: Keep only matches
    -- =========================
    detected_techs AS (
        SELECT 
            company_normalized,
            company,
            week_start,
            title,
            location,
            seniority,
            tech_name
        FROM tech_matches
        WHERE has_tech = true
    ),

    -- Step 5: Aggregate detected techs
    -- =================================
    aggregated_with_techs AS (
        SELECT 
            company_normalized,
            company,
            week_start,
            COUNT(DISTINCT title) as jobs_posted,
            
            COALESCE(
                LIST(DISTINCT tech_name ORDER BY tech_name),
                CAST([] AS VARCHAR[])
            ) as tech_array,
            
            COALESCE(
                LIST(DISTINCT location ORDER BY location) FILTER (WHERE location IS NOT NULL),
                CAST([] AS VARCHAR[])
            ) as locations,
            
            COUNT(DISTINCT title) FILTER (WHERE seniority = 'Senior') as senior_count,
            COUNT(DISTINCT title) FILTER (WHERE seniority = 'Junior') as junior_count,
            COUNT(DISTINCT title) FILTER (WHERE seniority = 'Lead') as lead_count,
            COUNT(DISTINCT title) FILTER (WHERE seniority = 'Mid-Level') as mid_count
            
        FROM detected_techs
        GROUP BY company_normalized, company, week_start
    ),

    -- Step 6: Find companies with NO detected techs
    -- ==============================================
    -- Use LEFT JOIN instead of NOT IN
    companies_without_techs AS (
        SELECT 
            w.company_normalized,
            w.company,
            w.week_start,
            COUNT(DISTINCT w.title) as jobs_posted,
            CAST([] AS VARCHAR[]) as tech_array,
            COALESCE(
                LIST(DISTINCT w.location ORDER BY w.location) FILTER (WHERE w.location IS NOT NULL),
                CAST([] AS VARCHAR[])
            ) as locations,
            COUNT(DISTINCT w.title) FILTER (
                WHERE CASE 
                    WHEN LOWER(w.title) LIKE '%senior%' THEN 'Senior'
                    WHEN LOWER(w.title) LIKE '%junior%' THEN 'Junior'
                    WHEN LOWER(w.title) LIKE '%lead%' THEN 'Lead'
                    ELSE 'Mid-Level'
                END = 'Senior'
            ) as senior_count,
            COUNT(DISTINCT w.title) FILTER (
                WHERE CASE 
                    WHEN LOWER(w.title) LIKE '%senior%' THEN 'Senior'
                    WHEN LOWER(w.title) LIKE '%junior%' THEN 'Junior'
                    WHEN LOWER(w.title) LIKE '%lead%' THEN 'Lead'
                    ELSE 'Mid-Level'
                END = 'Junior'
            ) as junior_count,
            COUNT(DISTINCT w.title) FILTER (
                WHERE CASE 
                    WHEN LOWER(w.title) LIKE '%senior%' THEN 'Senior'
                    WHEN LOWER(w.title) LIKE '%junior%' THEN 'Junior'
                    WHEN LOWER(w.title) LIKE '%lead%' THEN 'Lead'
                    ELSE 'Mid-Level'
                END = 'Lead'
            ) as lead_count,
            COUNT(DISTINCT w.title) FILTER (
                WHERE CASE 
                    WHEN LOWER(w.title) LIKE '%senior%' THEN 'Senior'
                    WHEN LOWER(w.title) LIKE '%junior%' THEN 'Junior'
                    WHEN LOWER(w.title) LIKE '%lead%' THEN 'Lead'
                    ELSE 'Mid-Level'
                END = 'Mid-Level'
            ) as mid_count
        FROM weekly_jobs w
        LEFT JOIN detected_techs dt 
            ON w.company_normalized = dt.company_normalized 
            AND w.week_start = dt.week_start
        WHERE dt.company_normalized IS NULL  -- Only companies NOT in detected_techs
        GROUP BY w.company_normalized, w.company, w.week_start
    )

    -- Step 7: Combine both groups
    -- ============================
    SELECT * FROM aggregated_with_techs

    UNION ALL

    SELECT * FROM companies_without_techs

    ORDER BY week_start DESC, jobs_posted DESC
    """
    
    # Execute query
    df = pl.read_database(query=query, connection=conn)
    
    context.log.info(f"Generated {len(df)} company-week statistics")
    
    # Convert array to string
    df = df.with_columns([
        pl.col("tech_array").list.join(",").fill_null("").alias("tech_stack")
    ])
    
    # Write to database
    context.log.info("Clearing old company_stats data")
    conn.execute("DELETE FROM company_stats")
    
    rows = [
        (
            row["company_normalized"],
            row["company"],
            row["week_start"],
            row["jobs_posted"],
            row["tech_stack"],
        )
        for row in df.to_dicts()
    ]
    
    context.log.info(f"Inserting {len(rows)} company-week records")
    conn.executemany(
        """
        INSERT INTO company_stats (
            company_normalized, company, week_start, jobs_posted, tech_stack
        )
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )
    
    # Metadata
    total_companies = df.select(pl.col("company_normalized").n_unique()).item()
    total_weeks = df.select(pl.col("week_start").n_unique()).item()
    avg_jobs_per_week = df.select(pl.col("jobs_posted").mean()).item()
    
    top_companies = (
        df.group_by("company_normalized")
        .agg([pl.col("jobs_posted").sum().alias("total_jobs")])
        .sort("total_jobs", descending=True)
        .head(5)
    )
    
    # Tech distribution
    tech_distribution = {}
    for row in df.to_dicts():
        if row["tech_stack"]:
            for tech in row["tech_stack"].split(","):
                tech_distribution[tech] = tech_distribution.get(tech, 0) + 1
    
    date_range = f"{df['week_start'].min()} to {df['week_start'].max()}"
    
    context.log.info(
        f"✅ Aggregation complete: {total_companies} companies, "
        f"{total_weeks} weeks, avg {avg_jobs_per_week:.1f} jobs/week"
    )
    
    return Output(
        value=len(df),
        metadata={
            "total_companies": total_companies,
            "total_weeks": total_weeks,
            "avg_jobs_per_week": round(avg_jobs_per_week, 2),
            "date_range": date_range,
            "technologies_configured": tech_count,
            "technologies_detected": len(tech_distribution),
            "top_companies": MetadataValue.md(
                "\n".join([
                    f"- **{row['company_normalized']}**: {row['total_jobs']} jobs"
                    for row in top_companies.to_dicts()
                ])
            ),
            "tech_distribution": MetadataValue.json(tech_distribution),
        },
    )