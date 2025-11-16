"""Job scraper with sample data fallback."""

from typing import Any
import structlog
from scraper_fallback import SampleJobGenerator

logger = structlog.get_logger()


def scrape_toronto_jobs(limit: int = 100) -> list[dict[str, Any]]:
    """Scrape Toronto jobs (using sample data for now).

    Note: Indeed RSS is currently blocking requests (403 Forbidden).
    We're using realistic sample data for pipeline development.
    Real sources (LinkedIn Jobs API, Greenhouse, Lever) will be added in Week 5.

    Args:
        limit: Maximum number of jobs to generate

    Returns:
        List of job dictionaries
    """
    logger.info("generating_sample_jobs", 
                limit=limit,
                note="Using sample data - Indeed RSS is blocked")
    
    generator = SampleJobGenerator()
    jobs = generator.generate(count=limit)
    
    logger.info("jobs_generated", count=len(jobs))
    return jobs


if __name__ == "__main__":
    import json
    import structlog

    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ]
    )

    jobs = scrape_toronto_jobs(limit=20)

    print(f"\n✅ Generated {len(jobs)} sample jobs")
    print(f"Source: {jobs[0]['source']}")
    
    print("\n📋 Sample job:")
    print(json.dumps(jobs[0], indent=2))
    
    print("\n🏢 Companies (top 10):")
    companies = {}
    for job in jobs:
        companies[job['company']] = companies.get(job['company'], 0) + 1
    
    for company, count in sorted(companies.items(), key=lambda x: -x[1])[:10]:
        print(f"  - {company}: {count} job(s)")
    
    print("\n�� Note: Using sample data while building the pipeline.")
    print("   Real job sources will be added later.")
