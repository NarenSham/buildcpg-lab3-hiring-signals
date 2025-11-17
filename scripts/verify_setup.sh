#!/bin/bash
# Verify Lab 3 setup is correct

set -e  # Exit on error

echo "🔍 Verifying Lab 3 Setup..."
echo ""

# Check Python version
echo "1. Checking Python version..."
python_version=$(python --version 2>&1)
if [[ $python_version == *"Python 3.1"* ]] || [[ $python_version == *"Python 3.2"* ]]; then
    echo "   ✅ $python_version"
else
    echo "   ❌ Python 3.10+ required. Found: $python_version"
    exit 1
fi

# Check virtual environment
echo "2. Checking virtual environment..."
if [[ "$VIRTUAL_ENV" != "" ]]; then
    echo "   ✅ Virtual environment active: $VIRTUAL_ENV"
else
    echo "   ❌ Virtual environment not activated"
    echo "   Run: source venv/bin/activate"
    exit 1
fi

# Check dependencies
echo "3. Checking dependencies..."
required_packages=("duckdb" "dagster" "polars" "pytest")
missing_packages=()

for package in "${required_packages[@]}"; do
    if python -c "import $package" 2>/dev/null; then
        echo "   ✅ $package installed"
    else
        echo "   ❌ $package not installed"
        missing_packages+=($package)
    fi
done

if [ ${#missing_packages[@]} -gt 0 ]; then
    echo ""
    echo "Missing packages: ${missing_packages[*]}"
    echo "Run: pip install -r requirements.txt"
    exit 1
fi

# Check database
echo "4. Checking database..."
if [ -f "warehouse/hiring_signals.duckdb" ]; then
    echo "   ✅ Database file exists"
    
    # Check tables
    tables=$(python -c "import duckdb; conn=duckdb.connect('warehouse/hiring_signals.duckdb'); print(len(conn.execute('SHOW TABLES').fetchall()))")
    if [ "$tables" -gt 0 ]; then
        echo "   ✅ Database has $tables tables"
    else
        echo "   ⚠️  Database has no tables. Run: make db-init"
    fi
else
    echo "   ⚠️  Database not found. Run: make db-init"
fi

# Check .gitignore
echo "5. Checking .gitignore..."
if grep -q "\.duckdb" .gitignore 2>/dev/null; then
    echo "   ✅ .duckdb files ignored"
else
    echo "   ❌ .duckdb not in .gitignore!"
    echo "   Add to .gitignore:"
    echo "      *.duckdb"
    echo "      *.duckdb.wal"
fi

echo ""
echo "✅ Setup verification complete!"
echo ""
echo "Next steps:"
echo "  make dagster-dev    # Start Dagster UI"
echo "  make test           # Run tests"
