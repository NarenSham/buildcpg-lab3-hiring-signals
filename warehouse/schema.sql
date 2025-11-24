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
    first_scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cleaned and deduplicated jobs
CREATE TABLE IF NOT EXISTS cleaned_jobs (
    job_id VARCHAR PRIMARY KEY,
    company VARCHAR NOT NULL,
    company_normalized VARCHAR,  
    title VARCHAR NOT NULL,
    title_normalized VARCHAR,     
    description TEXT,
    location VARCHAR,
    posting_date DATE,
    url VARCHAR,                  
    source VARCHAR,
    first_seen TIMESTAMP,
    last_seen TIMESTAMP
);

-- Technology detection configuration (SINGLE SOURCE OF TRUTH)
-- =============================================================
CREATE TABLE IF NOT EXISTS tech_config (
    tech_name VARCHAR PRIMARY KEY,
    keywords VARCHAR[],                -- Array of search patterns
    category VARCHAR,                  -- 'language', 'framework', 'cloud', etc.
    is_target BOOLEAN DEFAULT false,   -- Is this a target tech for scoring?
    score_weight DOUBLE DEFAULT 1.0    -- Weight for scoring
);

-- Seed with initial tech stack
INSERT INTO tech_config (tech_name, keywords, category, is_target, score_weight) VALUES
    ('Python', ARRAY['python', 'django', 'flask', 'fastapi'], 'language', true, 1.0),
    ('Java', ARRAY['java', 'spring', 'springboot'], 'language', false, 0.5),
    ('JavaScript', ARRAY['javascript', 'js ', 'node.js', 'nodejs'], 'language', false, 0.5),
    ('TypeScript', ARRAY['typescript', 'ts '], 'language', true, 0.8),
    ('React', ARRAY['react', 'reactjs', 'react.js'], 'framework', true, 1.0),
    ('Vue', ARRAY['vue', 'vuejs', 'vue.js'], 'framework', false, 0.5),
    ('Angular', ARRAY['angular', 'angularjs'], 'framework', false, 0.3),
    ('AWS', ARRAY['aws', 'amazon web services', 'ec2', 's3', 'lambda'], 'cloud', false, 0.7),
    ('Azure', ARRAY['azure', 'microsoft azure'], 'cloud', false, 0.5),
    ('GCP', ARRAY['gcp', 'google cloud', 'bigquery'], 'cloud', false, 0.7),
    ('Kubernetes', ARRAY['kubernetes', 'k8s'], 'devops', false, 0.8),
    ('Docker', ARRAY['docker', 'containerization'], 'devops', false, 0.6),
    ('Go', ARRAY['golang', 'go '], 'language', false, 0.6),
    ('Rust', ARRAY['rust'], 'language', false, 0.7),
    ('PostgreSQL', ARRAY['postgresql', 'postgres', 'psql'], 'database', false, 0.5),
    ('MongoDB', ARRAY['mongodb', 'mongo'], 'database', false, 0.4)
ON CONFLICT (tech_name) DO NOTHING;

-- Company statistics (aggregated weekly)
CREATE TABLE IF NOT EXISTS company_stats (
    company_normalized VARCHAR,
    company VARCHAR,
    week_start DATE,
    jobs_posted INTEGER,
    tech_stack TEXT,
    PRIMARY KEY (company_normalized, week_start)
);

-- Lead scores
CREATE TABLE IF NOT EXISTS lead_scores (
    company_normalized VARCHAR,
    company VARCHAR,
    week_start DATE,
    jobs_this_week INTEGER,
    jobs_last_week INTEGER,
    velocity_score DOUBLE,
    tech_match_score DOUBLE,
    volume_score DOUBLE,
    composite_score DOUBLE,
    score_metadata TEXT,
    PRIMARY KEY (company_normalized, week_start)
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_raw_jobs_company ON raw_jobs(company);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_first_scraped ON raw_jobs(first_scraped_at);
CREATE INDEX IF NOT EXISTS idx_raw_jobs_last_scraped ON raw_jobs(last_scraped_at);
CREATE INDEX IF NOT EXISTS idx_cleaned_jobs_company ON cleaned_jobs(company);
CREATE INDEX IF NOT EXISTS idx_company_stats_week ON company_stats(week_start);
CREATE INDEX IF NOT EXISTS idx_lead_scores_composite ON lead_scores(composite_score DESC);