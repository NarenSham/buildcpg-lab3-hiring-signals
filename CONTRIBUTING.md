# Contributing to Lab 3

Thank you for your interest in contributing!

## Development Setup
```bash
# Clone repository
git clone https://github.com/NarenSham/buildcpg-lab3-hiring-signals.git
cd buildcpg-lab3-hiring-signals

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies (including dev dependencies)
pip install -r requirements.txt -r requirements-dev.txt

# Initialize database
make db-init

# Run tests
make test
```

## Code Quality

Before committing:
```bash
# Format code
make format

# Lint code
make lint

# Run tests
make test
```

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` - New features
- `fix:` - Bug fixes
- `docs:` - Documentation only
- `chore:` - Maintenance tasks
- `refactor:` - Code refactoring
- `test:` - Adding tests

Example:
```
feat(assets): Add company_stats aggregation asset

- Aggregate jobs by company and week
- Extract tech stack from descriptions
- Calculate hiring velocity
```

## Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing`)
3. Make your changes
4. Run tests and linting
5. Commit your changes
6. Push to your fork
7. Open a Pull Request

## Questions?

Open an issue or reach out on [LinkedIn](https://linkedin.com/in/narensham).
