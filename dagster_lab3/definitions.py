"""Dagster definitions - the entry point for the Dagster project."""

from dagster import Definitions, load_assets_from_modules
from dagster_lab3.resources import DuckDBResource
from dagster_lab3 import assets

# Load all assets
all_assets = load_assets_from_modules([assets])

# Define resources
resources = {
    "duckdb": DuckDBResource(db_path="warehouse/hiring_signals.duckdb"),
}

# Create definitions object
defs = Definitions(
    assets=all_assets,
    resources=resources,
)
