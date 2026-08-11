# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

dbt-coves is a CLI tool that automates tedious aspects of dbt development. It generates sources, staging models, properties files, Airflow DAGs, and handles Airbyte/Fivetran integration, blue-green deployments, and data synchronization.

## Common Commands

### Development Setup
```bash
uv sync --all-groups    # Install dependencies (dev + test groups)
source .venv/bin/activate  # Activate virtual environment (or prefix commands with `uv run`)
```

### Running Tests
```bash
uv run pytest tests/                              # Run all tests
uv run pytest tests/generate_sources_test.py     # Run specific test file
uv run pytest tests/file.py::test_name -v        # Run specific test function
uv run pytest --cov=dbt_coves tests/             # Run with coverage
uv run tox                                        # Test against multiple dbt versions (1.10, 1.11, latest)
```

### Linting & Formatting
```bash
pre-commit run --all-files    # Run all linters (ruff check, ruff format)
uv run ruff format dbt_coves/ # Format code (line-length=88)
uv run ruff check --fix dbt_coves/  # Lint + sort imports
uv run mypy                   # Type check
```

### CLI Usage
```bash
dbt-coves --help
dbt-coves generate sources --help
dbt-coves generate properties --help
```

## Architecture

### Task-Based Command System
All CLI commands are implemented as Task classes inheriting from base classes in
`dbt_coves/tasks/base.py` (`BaseTask`, `NonDbtBaseTask`, `NonDbtBaseConfiguredTask`) and
`dbt_coves/tasks/base_configured.py` (`BaseConfiguredTask`):
- `BaseTask`: Lightweight tasks not requiring dbt config
- `BaseConfiguredTask`: Tasks using dbt configuration and adapters
- `NonDbtBaseTask`/`NonDbtBaseConfiguredTask`: Tasks using only dbt-coves config

Each task registers its own argparse subparser via `register_parser()` and executes via `run()`.

### Entry Point Flow
`dbt_coves/core/main.py` → `main()` → `handle()` → parses args → instantiates task → `task.run()`

### Task Hierarchy
`GenerateTask` is a parent task with sub-tasks:
- `GenerateSourcesTask` - Generate dbt sources from database metadata
- `GeneratePropertiesTask` - Generate YAML properties for models
- `GenerateMetadataTask` - Extract metadata as CSV
- `GenerateAirflowDagsTask` - Generate Airflow DAGs from YAML configs
- `GenerateDocsTask` - Generate dbt documentation
- `GenerateTemplatesTask` - Generate project templates

### Configuration System
- Project config loaded from `.dbt_coves.yml` (YAML)
- Pydantic models in `dbt_coves/config/config.py` define and validate config structure
- Each command has its own config model (e.g., `GenerateSourcesModel`, `GeneratePropertiesModel`)

### dbt Integration
- Uses dbt-core's adapter system for Snowflake, Redshift, BigQuery
- Compatible with dbt versions 1.1 - 1.10
- Handles version differences with conditional imports:
```python
try:
    from dbt.flags import set_flags
    SET_FLAGS = True
except ImportError:
    SET_FLAGS = False
```

### Code Generation
- Jinja2 templates in `dbt_coves/templates/`
- Update strategies control overwriting: ask/overwrite/skip

## Key Directories

- `dbt_coves/tasks/` - Command implementations
- `dbt_coves/config/` - Pydantic configuration models
- `dbt_coves/utils/` - Utilities (YAML, Jinja, API, secrets, shell)
- `dbt_coves/templates/` - Jinja templates for code generation
- `tests/` - pytest test suite
- `docs/commands/` - Command documentation

## Testing Notes

Integration tests require database credentials. Set up via environment variables or 1Password (see `tests/generate_onepsw_env_file.py`).

Required env vars for full integration testing:
- Snowflake: `USER_SNOWFLAKE`, `PASSWORD_SNOWFLAKE`, `ACCOUNT_SNOWFLAKE`, `WAREHOUSE_SNOWFLAKE`, `DATABASE_SNOWFLAKE`, `SCHEMA_SNOWFLAKE`
- Redshift: `HOST_REDSHIFT`, `USER_REDSHIFT`, `PASSWORD_REDSHIFT`, `DATABASE_REDSHIFT`, `SCHEMA_REDSHIFT`
- BigQuery: `PROJECT_BIGQUERY`, `DATASET_BIGQUERY`, `SERVICE_ACCOUNT_GCP`

## Git Conventions

- Branch naming: `DCV-XXXX-description` (Jira ticket format)
- PR titles should match branch naming convention
- Pre-commit hooks enforce code style - install with `pre-commit install`
