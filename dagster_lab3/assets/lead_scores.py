"""Lead scoring asset - identify hot hiring companies."""

from dagster import asset, AssetExecutionContext, Output, MetadataValue, asset_check, AssetCheckResult
from dagster_lab3.resources import DuckDBResource
import polars as pl


@asset(
    deps=["company_stats_asset"],
    description="Config-driven lead scoring based on velocity, tech match, and volume",
    group_name="analytics",
)
def lead_scores_asset(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> Output[int]:
    """
    Calculate composite lead scores for companies.
    
    SCORING ALGORITHM
    =================
    
    1. VELOCITY SCORE (40% weight)
       - How fast is hiring growing week-over-week?
       - Formula: (jobs_this_week - jobs_last_week) / jobs_last_week * 100
       - Capped at 100
    
    2. TECH MATCH SCORE (35% weight)
       - Uses tech_config.is_target to identify target technologies
       - Weighted by tech_config.score_weight
       - Formula: (matched_weight / max_possible_weight) * 100
    
    3. VOLUME SCORE (25% weight)
       - Absolute number of jobs posted
       - Normalized: (company_jobs / max_jobs) * 100
    
    COMPOSITE = 0.4 * velocity + 0.35 * tech_match + 0.25 * volume
    
    To change target techs:
    -----------------------
    UPDATE tech_config SET is_target = true WHERE tech_name = 'Go';
    
    Then re-materialize this asset.
    """
    conn = duckdb.get_connection()
    
    context.log.info("Starting lead scoring algorithm")
    
    # Check we have target techs configured
    target_techs = conn.execute(
        "SELECT tech_name, score_weight FROM tech_config WHERE is_target = true ORDER BY tech_name"
    ).fetchall()
    
    if len(target_techs) == 0:
        context.log.error("❌ No target technologies configured! Set is_target=true in tech_config.")
        return Output(
            value=0,
            metadata={"error": "No target technologies configured"}
        )
    
    context.log.info(f"🎯 Target technologies: {[t[0] for t in target_techs]}")
    
    # ============================================================================
    # CONFIG-DRIVEN SCORING QUERY
    # ============================================================================
    
    query = """
    -- Step 1: Get target tech configuration
    -- ======================================
    WITH target_techs AS (
        SELECT 
            tech_name,
            score_weight
        FROM tech_config
        WHERE is_target = true
    ),
    
    -- Step 2: Calculate maximum possible tech score
    -- =============================================
    max_tech_score AS (
        SELECT SUM(score_weight) as max_score
        FROM target_techs
    ),
    
    -- Step 3: Get weekly stats with previous week
    -- ===========================================
    weekly_stats AS (
        SELECT 
            company_normalized,
            company,
            week_start,
            jobs_posted,
            tech_stack,
            
            -- Get last week's job count using LAG window function
            LAG(jobs_posted, 1) OVER (
                PARTITION BY company_normalized
                ORDER BY week_start
            ) as jobs_last_week
            
        FROM company_stats
        ORDER BY week_start DESC
    ),
    
    -- Step 4: Calculate tech match score for each company
    -- ===================================================
    company_tech_scores AS (
        SELECT 
            w.company_normalized,
            w.company,
            w.week_start,
            w.jobs_posted,
            w.jobs_last_week,
            w.tech_stack,
            
            -- Calculate tech match score
            -- ===========================
            -- For each target tech, check if company uses it
            -- Sum up the weights of matched techs
            COALESCE(
                (
                    SELECT SUM(t.score_weight)
                    FROM target_techs t
                    WHERE w.tech_stack LIKE '%' || t.tech_name || '%'
                ),
                0
            ) as matched_tech_weight,
            
            -- Get max possible score
            (SELECT max_score FROM max_tech_score) as max_possible_weight
            
        FROM weekly_stats w
    ),
    
    -- Step 5: Calculate individual scores
    -- ===================================
    scored AS (
        SELECT 
            company_normalized,
            company,
            week_start,
            jobs_posted as jobs_this_week,
            COALESCE(jobs_last_week, 0) as jobs_last_week,
            
            -- VELOCITY SCORE (40%)
            -- ====================
            -- Growth rate as percentage, capped at 100
            CASE 
                WHEN jobs_last_week = 0 OR jobs_last_week IS NULL THEN 100
                ELSE LEAST(
                    ((jobs_posted - jobs_last_week)::FLOAT / jobs_last_week * 100),
                    100
                )
            END as velocity_score,
            
            -- TECH MATCH SCORE (35%)
            -- ======================
            -- Config-driven! Reads from tech_config.is_target
            CASE 
                WHEN max_possible_weight = 0 THEN 0
                ELSE (matched_tech_weight / max_possible_weight * 100)
            END as tech_match_score,
            
            -- VOLUME SCORE (25%)
            -- ==================
            -- Normalized by max jobs across all companies
            (jobs_posted::FLOAT / MAX(jobs_posted) OVER () * 100) as volume_score,
            
            tech_stack
        FROM company_tech_scores
    )
    
    -- Step 6: Calculate composite score
    -- =================================
    SELECT 
        company_normalized,
        company,
        week_start,
        jobs_this_week,
        jobs_last_week,
        
        ROUND(velocity_score, 2) as velocity_score,
        ROUND(tech_match_score, 2) as tech_match_score,
        ROUND(volume_score, 2) as volume_score,
        
        -- COMPOSITE SCORE: Weighted average
        ROUND(
            (velocity_score * 0.4 +      -- 40% velocity
             tech_match_score * 0.35 +   -- 35% tech match
             volume_score * 0.25),       -- 25% volume
            2
        ) as composite_score,
        
        tech_stack
    FROM scored
    WHERE week_start = (SELECT MAX(week_start) FROM scored)  -- Only latest week
    ORDER BY composite_score DESC
    """
    
    # Execute query
    df = pl.read_database(query=query, connection=conn)
    
    if len(df) == 0:
        context.log.warning("⚠️  No companies scored - check if company_stats has data")
        return Output(
            value=0,
            metadata={"warning": "No data to score"}
        )
    
    context.log.info(f"Scored {len(df)} companies for week {df['week_start'][0]}")
    
    # ============================================================================
    # Write to database
    # ============================================================================
    
    context.log.info("Clearing old lead_scores data")
    conn.execute("DELETE FROM lead_scores")
    
    rows = [
        (
            row["company_normalized"],
            row["company"],
            row["week_start"],
            row["jobs_this_week"],
            row["jobs_last_week"],
            row["velocity_score"],
            row["tech_match_score"],
            row["volume_score"],
            row["composite_score"],
            row["tech_stack"],
        )
        for row in df.to_dicts()
    ]
    
    context.log.info(f"Writing {len(rows)} lead scores to database")
    conn.executemany(
        """
        INSERT INTO lead_scores (
            company_normalized, company, week_start,
            jobs_this_week, jobs_last_week,
            velocity_score, tech_match_score, volume_score,
            composite_score, score_metadata
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    
   # ============================================================================
    # Generate insights for metadata
    # ============================================================================
    
    # Get top 10 leads
    top_leads = df.head(10)
    
    # Convert to list of dicts for easier access
    top_leads_dicts = top_leads.to_dicts()
    
    # Score distribution
    high_score_count = len(df.filter(pl.col("composite_score") >= 80))
    medium_score_count = len(df.filter(
        (pl.col("composite_score") >= 50) & (pl.col("composite_score") < 80)
    ))
    low_score_count = len(df.filter(pl.col("composite_score") < 50))
    
    # Component averages (these return scalars already)
    avg_velocity = float(df["velocity_score"].mean())
    avg_tech_match = float(df["tech_match_score"].mean())
    avg_volume = float(df["volume_score"].mean())
    avg_composite = float(df["composite_score"].mean())
    
    # Create top leads table for Dagster UI
    top_leads_table = "| Rank | Company | Score | Velocity | Tech | Volume | Jobs | Tech Stack |\n"
    top_leads_table += "|------|---------|-------|----------|------|--------|------|------------|\n"
    for i, row in enumerate(top_leads_dicts, 1):
        tech_preview = row['tech_stack'][:30] + "..." if len(row['tech_stack']) > 30 else row['tech_stack']
        top_leads_table += (
            f"| {i} | {row['company']} | **{row['composite_score']}** | "
            f"{row['velocity_score']} | {row['tech_match_score']} | "
            f"{row['volume_score']} | {row['jobs_this_week']} | {tech_preview} |\n"
        )
    
    context.log.info(
        f"📊 Score distribution: {high_score_count} hot (≥80), "
        f"{medium_score_count} warm (50-79), {low_score_count} cold (<50)"
    )
    
    # Get top lead info safely (handle empty case)
    top_lead = top_leads_dicts[0] if len(top_leads_dicts) > 0 else None
    
    return Output(
        value=len(df),
        metadata={
            "total_companies_scored": len(df),
            "scoring_date": str(df["week_start"][0]) if len(df) > 0 else "N/A",
            "target_techs": [t[0] for t in target_techs],
            
            # Score distribution
            "hot_leads_count": high_score_count,
            "warm_leads_count": medium_score_count,
            "cold_leads_count": low_score_count,
            
            # Component averages
            "avg_velocity_score": round(avg_velocity, 2),
            "avg_tech_match_score": round(avg_tech_match, 2),
            "avg_volume_score": round(avg_volume, 2),
            "avg_composite_score": round(avg_composite, 2),
            
            # Top lead
            "top_lead_company": top_lead["company"] if top_lead else "None",
            "top_lead_score": float(top_lead["composite_score"]) if top_lead else 0.0,
            
            # Top 10 table (displays nicely in Dagster UI)
            "top_10_leads": MetadataValue.md(top_leads_table),
        },
    )
# ============================================================================
# DATA QUALITY CHECKS
# ============================================================================

@asset_check(
    asset=lead_scores_asset,
    description="Ensure target techs are configured"
)
def check_target_techs_configured(duckdb: DuckDBResource) -> AssetCheckResult:
    """Verify that at least one target tech is configured."""
    conn = duckdb.get_connection()
    
    count = conn.execute(
        "SELECT COUNT(*) FROM tech_config WHERE is_target = true"
    ).fetchone()[0]
    
    passed = count > 0
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "target_tech_count": count,
        },
        severity="ERROR" if not passed else None,
    )


@asset_check(
    asset=lead_scores_asset,
    description="Ensure we have leads to work with"
)
def check_minimum_leads(duckdb: DuckDBResource) -> AssetCheckResult:
    """Data quality check: Verify we have enough scoreable companies."""
    conn = duckdb.get_connection()
    
    count = conn.execute("SELECT COUNT(*) FROM lead_scores").fetchone()[0]
    
    min_expected = 5
    passed = count >= min_expected
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "lead_count": count,
            "min_expected": min_expected,
        },
        severity="WARN" if not passed else None,
    )


@asset_check(
    asset=lead_scores_asset,
    description="Verify score validity"
)
def check_score_validity(duckdb: DuckDBResource) -> AssetCheckResult:
    """Data quality check: Ensure all scores are in valid range [0, 100]."""
    conn = duckdb.get_connection()
    
    invalid_scores = conn.execute("""
        SELECT 
            company,
            velocity_score,
            tech_match_score,
            volume_score,
            composite_score
        FROM lead_scores
        WHERE velocity_score < 0 OR velocity_score > 100
           OR tech_match_score < 0 OR tech_match_score > 100
           OR volume_score < 0 OR volume_score > 100
           OR composite_score < 0 OR composite_score > 100
           OR velocity_score IS NULL
           OR tech_match_score IS NULL
           OR volume_score IS NULL
           OR composite_score IS NULL
    """).fetchall()
    
    passed = len(invalid_scores) == 0
    
    return AssetCheckResult(
        passed=passed,
        metadata={
            "invalid_count": len(invalid_scores),
            "examples": str(invalid_scores[:3]) if invalid_scores else "None",
        },
        severity="ERROR" if not passed else None,
    )