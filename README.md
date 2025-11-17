# Lab 3: Hiring Signals & Lead Discovery Pipeline

> A zero-cost, weekly-updating data pipeline that detects hiring signals from Toronto job boards and generates qualified leads with enrichment data.

[... keep existing badges and intro ...]

---

## 🚀 Quick Start

### Prerequisites

- **Python 3.10+** (check: `python3 --version`)
- Git
- 5 minutes

### Installation
```bash
# Clone repository
git clone https://github.com/NarenSham/buildcpg-lab3-hiring-signals.git
cd buildcpg-lab3-hiring-signals

# Create virtual environment (use python3, not python)
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # On macOS/Linux
# OR
venv\Scripts\activate     # On Windows

# Verify Python version
python --version  # Should show 3.10+

# Install dependencies
pip install -r requirements.txt

# Initialize database
make db-init

# Start Dagster UI
make dagster-dev
```

**Open browser:** http://localhost:3000

---

## 🐛 Troubleshooting

### "No module named venv" Error

**Problem:** Using Python 2.7 instead of Python 3

**Solution:**
```bash
# Use python3 explicitly
python3 -m venv venv
source venv/bin/activate

# OR use pyenv to set Python version
pyenv local 3.11.9
python -m venv venv
source venv/bin/activate
```

### "Command not found: dagster"

**Problem:** Virtual environment not activated

**Solution:**
```bash
# Activate venv first
source venv/bin/activate

# Then run make command
make dagster-dev
```

### "ImportError: No module named duckdb"

**Problem:** Dependencies not installed

**Solution:**
```bash
# Ensure venv is activated
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Verify installation
pip list | grep duckdb
```

---

[... rest of README ...]
