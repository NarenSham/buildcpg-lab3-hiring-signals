# Lab 3: Hiring Signals Pipeline

> Detect hiring signals from Toronto tech job boards and generate qualified sales leads

**The Problem:** Sales teams waste time cold-calling companies that aren't hiring. Hiring is a strong buying signal for B2B products (recruiting tools, HR software, etc.).

**This Solution:** An automated pipeline that:
1. Scrapes Toronto tech job boards weekly
2. Identifies companies with hiring velocity (5+ jobs in 2 weeks = hot lead)
3. Scores leads based on tech stack match and hiring speed
4. Enriches top 10 leads with company data and decision-maker contacts
5. Exports to CRM-ready CSV

**Tech Stack:**
- **Dagster** - Asset-based orchestration (not task-based like Airflow)
- **DuckDB** - Embedded analytics database (4x faster than SQLite, no server needed)
- **Polars** - High-performance dataframes (8x faster than Pandas)

---

## 🚀 Quick Start (5 minutes)
```bash
# Clone and setup
git clone https://github.com/NarenSham/buildcpg-lab3-hiring-signals.git
cd buildcpg-lab3-hiring-signals

# Create virtual environment (use Python 3.11 or 3.12)
python3.11 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Initialize database
make db-init

# Start Dagster UI
make dagster-dev
```

Open http://localhost:3000 → Click "Assets" → Click "Materialize All"

---

## 📊 What You'll See

After materializing assets:
```
Pipeline: raw_jobs → cleaned_jobs → company_stats → lead_scores
                                                         ↓
                                                   enriched_leads
```

**Metrics:**
- 100 raw jobs scraped
- 85 unique jobs after deduplication
- 20 companies analyzed
- Top 5 leads scored and enriched

---

## 🏗️ Architecture Decisions

**Why DuckDB over PostgreSQL?**
- No server setup needed (embedded)
- Columnar storage (4-8x faster for analytics)
- Perfect for 10K-1M row datasets
- [Full ADR](docs/adr/001-duckdb-over-postgres.md)

**Why Dagster over Airflow?**
- Asset-based (think "data", not "tasks")
- Better local development experience
- Built-in data quality checks
- [Full ADR](docs/adr/002-dagster-over-airflow.md)

**Why Polars over Pandas?**
- Written in Rust (8x faster)
- Lazy evaluation (optimized query plans)
- Better memory efficiency
- [Benchmark results](scripts/benchmark_polars.py)

---

## 📁 Project Structure
```
lab3_hiring_signals/
├── dagster_lab3/         # Dagster orchestration
│   ├── assets/           # Data assets (bronze → silver → gold)
│   ├── resources/        # Shared connections (DuckDB)
│   └── definitions.py    # Dagster entry point
├── src/                  # Core Python logic
│   ├── scraper.py        # Job scraper
│   └── loader.py         # DuckDB loader
├── warehouse/
│   ├── schema.sql        # Database DDL
│   └── *.duckdb          # DuckDB files (gitignored)
├── config/               # Configuration
├── tests/                # Tests
└── Makefile              # Commands
```

---

## 🎯 Current Status

**Week 1 Complete ✅**
- [x] Sample data generator (100 Toronto tech jobs)
- [x] DuckDB warehouse setup
- [x] Dagster pipeline (raw_jobs → cleaned_jobs)
- [x] Data quality checks

**Next Up (Week 2):**
- [ ] company_stats asset (aggregate by company)
- [ ] Tech stack extraction
- [ ] lead_scores asset
- [ ] Score explainability

---

## 💡 Key Learnings

This project demonstrates:

1. **Asset-based orchestration** - Dagster's dependency management
2. **Columnar analytics** - Why DuckDB beats row-based databases for analytics
3. **Performance optimization** - Polars vs Pandas benchmarks
4. **Data quality engineering** - Asset checks, idempotency
5. **Pragmatic decisions** - Sample data during development, real APIs in production

---

## 🐛 Troubleshooting

**Error: "No module named 'pyarrow'"**
```bash
pip install pyarrow
```

**Dagster won't start**
```bash
# Check Python version
python --version  # Should be 3.11.x or 3.12.x

# Reinstall dependencies
pip install -r requirements.txt
```

**Database not found**
```bash
make db-init
```

---

## 🤝 Author

**Naren Sham**
- GitHub: [@NarenSham](https://github.com/NarenSham)
- Portfolio: [BuildCPG Labs](https://github.com/NarenSham/buildcpg-labs)

Part of a series demonstrating Staff/Distinguished-level data engineering skills.

---

**⭐ Star this repo if you find it helpful!**