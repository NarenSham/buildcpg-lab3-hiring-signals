"""Fallback scraper with sample data for testing."""

import hashlib
from datetime import datetime, timedelta
import random
from typing import Any
import structlog

logger = structlog.get_logger()


class SampleJobGenerator:
    """Generate realistic sample job data for testing."""

    COMPANIES = [
        "Shopify", "Wealthsimple", "TELUS Digital", "RBC", "TD Bank",
        "Loblaw Digital", "Rogers Communications", "Bell Canada", "Scotiabank",
        "BMO", "Stripe", "Amazon", "Google", "Microsoft", "Meta",
        "Uber", "Lyft", "Square", "Toast", "Lightspeed"
    ]

    JOB_TITLES = [
        "Senior Software Engineer - Backend",
        "Staff Software Engineer - Platform",
        "Principal Engineer - Infrastructure",
        "Senior Frontend Engineer",
        "Full Stack Developer",
        "DevOps Engineer",
        "Site Reliability Engineer",
        "Data Engineer",
        "ML Engineer",
        "Security Engineer",
        "Engineering Manager",
        "Technical Lead - Backend",
        "Software Engineer - Mobile",
        "Cloud Architect",
        "Distinguished Engineer"
    ]

    TECH_STACKS = [
        ["Python", "Django", "PostgreSQL", "Docker", "Kubernetes"],
        ["Java", "Spring", "MySQL", "AWS", "Terraform"],
        ["Go", "Kubernetes", "Prometheus", "gRPC"],
        ["TypeScript", "React", "Node.js", "GraphQL", "MongoDB"],
        ["Rust", "WebAssembly", "PostgreSQL"],
        ["Python", "FastAPI", "Redis", "Kafka", "Spark"],
    ]

    def generate(self, count: int = 100) -> list[dict[str, Any]]:
        """Generate sample jobs.

        Args:
            count: Number of jobs to generate

        Returns:
            List of job dictionaries
        """
        logger.info("generating_sample_jobs", count=count)

        jobs = []
        now = datetime.now()

        for i in range(count):
            company = random.choice(self.COMPANIES)
            title = random.choice(self.JOB_TITLES)
            tech_stack = random.choice(self.TECH_STACKS)
            
            # Generate posting date (within last 14 days)
            days_ago = random.randint(0, 14)
            posting_date = (now - timedelta(days=days_ago)).date()

            # Create job ID
            job_id = hashlib.sha256(
                f"{company}_{title}_{posting_date}_{i}".encode()
            ).hexdigest()[:16]

            # Generate description
            description = self._generate_description(title, company, tech_stack)

            jobs.append({
                "job_id": job_id,
                "company": company,
                "title": title,
                "description": description,
                "location": "Toronto, ON",
                "posting_date": posting_date.isoformat(),
                "url": f"https://example.com/jobs/{job_id}",
                "source": "sample_generator",
                "scraped_at": now.isoformat(),
            })

        logger.info("sample_jobs_generated", count=len(jobs))
        return jobs

    def _generate_description(
        self, 
        title: str, 
        company: str, 
        tech_stack: list[str]
    ) -> str:
        """Generate realistic job description."""
        
        return f"""
{company} is hiring a {title}.

Requirements:
- 5+ years of software engineering experience
- Strong experience with {', '.join(tech_stack[:3])}
- Experience with {tech_stack[-1]} is a plus
- Excellent communication skills
- Bachelor's degree in Computer Science or equivalent

Tech Stack:
{', '.join(tech_stack)}

Benefits:
- Competitive salary
- Health and dental coverage
- RRSP matching
- Flexible work arrangements
- Professional development budget

Location: Toronto, ON (Hybrid)
"""


def scrape_toronto_jobs(limit: int = 100) -> list[dict[str, Any]]:
    """Generate sample Toronto jobs for testing.

    Args:
        limit: Number of jobs to generate

    Returns:
        List of job dictionaries
    """
    generator = SampleJobGenerator()
    return generator.generate(count=limit)


if __name__ == "__main__":
    import structlog
    import json

    structlog.configure(
        processors=[
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ]
    )

    jobs = scrape_toronto_jobs(limit=20)

    print(f"\n✅ Generated {len(jobs)} sample jobs")
    print("\nSample job:")
    print(json.dumps(jobs[0], indent=2))
    
    print("\nCompanies:")
    companies = {}
    for job in jobs:
        companies[job['company']] = companies.get(job['company'], 0) + 1
    
    for company, count in sorted(companies.items(), key=lambda x: -x[1])[:10]:
        print(f"  - {company}: {count} job(s)")
