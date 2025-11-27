"""Export assets - generate dashboard-ready data files."""

from dagster import asset, AssetExecutionContext, Output, MetadataValue
from dagster_lab3.resources import DuckDBResource
import polars as pl
from pathlib import Path
from datetime import datetime
import json


@asset(
    deps=["lead_scores_asset"],
    description="Export lead scores and trends to CSV/JSON for dashboards",
    group_name="export",
)
def export_dashboard_data(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> Output[dict]:
    """Export processed data for dashboard consumption."""
    conn = duckdb.get_connection()
    
    # Create exports directory
    export_dir = Path("/app/warehouse/exports")
    export_dir.mkdir(exist_ok=True)
    
    context.log.info(f"Exporting to {export_dir}")
    
    # Export 1: Lead Scores
    # Note: tech_stack is in score_metadata column, not a separate column
    lead_scores = pl.read_database(
        """
        SELECT 
            company,
            composite_score,
            velocity_score,
            tech_match_score,
            volume_score,
            jobs_this_week,
            jobs_last_week,
            score_metadata as tech_stack,  -- This column contains tech stack
            week_start
        FROM lead_scores
        ORDER BY composite_score DESC
        """,
        connection=conn
    )
    
    lead_scores_path = export_dir / "lead_scores_latest.csv"
    lead_scores.write_csv(lead_scores_path)
    context.log.info(f"Exported {len(lead_scores)} lead scores")
    
    # Export 2: Company Trends
    company_trends = pl.read_database(
        """
        SELECT 
            company_normalized,
            company,
            week_start,
            jobs_posted,
            tech_stack
        FROM company_stats
        ORDER BY company_normalized, week_start
        """,
        connection=conn
    )
    
    trends_path = export_dir / "company_trends.csv"
    company_trends.write_csv(trends_path)
    context.log.info(f"Exported {len(company_trends)} trends")
    
    # Export 3: Summary
    summary_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_companies,
            SUM(CASE WHEN composite_score >= 80 THEN 1 ELSE 0 END) as hot_leads,
            SUM(CASE WHEN composite_score >= 50 AND composite_score < 80 THEN 1 ELSE 0 END) as warm_leads,
            SUM(CASE WHEN composite_score < 50 THEN 1 ELSE 0 END) as cold_leads,
            AVG(composite_score) as avg_score,
            MAX(week_start) as latest_week
        FROM lead_scores
    """).fetchone()
    
    summary = {
        "generated_at": datetime.now().isoformat(),
        "latest_week": str(summary_stats[5]) if summary_stats[5] else "N/A",
        "total_companies": summary_stats[0] or 0,
        "hot_leads": summary_stats[1] or 0,
        "warm_leads": summary_stats[2] or 0,
        "cold_leads": summary_stats[3] or 0,
        "avg_composite_score": round(summary_stats[4], 2) if summary_stats[4] else 0,
        "tech_distribution": {},
    }
    
    # Tech distribution from company_stats
    try:
        tech_dist = conn.execute("""
            SELECT 
                UNNEST(string_split(tech_stack, ',')) as tech,
                COUNT(*) as count
            FROM company_stats
            WHERE tech_stack != ''
            GROUP BY tech
            ORDER BY count DESC
        """).fetchall()
        
        summary["tech_distribution"] = {tech.strip(): count for tech, count in tech_dist}
    except Exception as e:
        context.log.warning(f"Could not get tech distribution: {e}")
    
    summary_path = export_dir / "summary.json"
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    context.log.info("Export complete!")
    
    return Output(
        value={
            "lead_scores_path": str(lead_scores_path),
            "trends_path": str(trends_path),
            "summary_path": str(summary_path),
        },
        metadata={
            "total_leads": len(lead_scores),
            "hot_leads": summary["hot_leads"],
            "export_timestamp": summary["generated_at"],
        },
    )
