-- Lab 3: Hiring Signals Database Schema
-- DuckDB version

-- Raw jobs from scraping
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
);

-- Cleaned and deduplicated jobs
CREATE TABLE IF NOT EXISTS cleaned_jobs (
    job_id VARCHAR PRIMARY KEY,
    company VARCHAR NOT NULL,
    title VARCHAR NOT NULL,
    description TEXT,
    location VARCHAR,
    posting_date DATE,
    source VARCHAR,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP
);

-- Company statistics (aggregated weekly)
CREATE TABLE IF NOT EXISTS company_stats (
    company VARCHAR,
    week_start DATE,
    jobs_posted INTEGER,
    tech_stack JSON,
    PRIMARY KEY (company, week_start)
);

-- Lead scores
CREATE TABLE IF NOT EXISTS lead_scores (
    company VARCHAR,
    week_start DATE,
    jobs_this_week INTEGER,
    jobs_last_week INTEGER,
    velocity_score DOUBLE,
    tech_match_score DOUBLE,
    composite_score DOUBLE,
    score_metadata JSON,
    PRIMARY KEY (company, week_start)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_raw_jobs_company ON raw_jobs(company);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_scraped_at ON raw_jobs(scraped_at);
CREATE INDEX IF NOT EXISTS idx_cleaned_jobs_company ON cleaned_jobs(company);
CREATE INDEX IF NOT EXISTS idx_company_stats_week ON company_stats(week_start);
CREATE INDEX IF NOT EXISTS idx_lead_scores_composite ON lead_scores(composite_score DESC);
