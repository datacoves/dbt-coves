## dbt-coves [0.21.0-a.17] - 2022-01-18

- Ask user if auto-fixing linting violations
- Upgrade sqlfluff to 0.9.1

## dbt-coves [0.21.0-a.11] - 2021-12-07

### Features

- Released 2 new commands: `dbt-coves extract airbyte` and `dbt-coves load airbyte`

## dbt-coves [0.21.0-a.9] - 2021-10-26

### Features

- Added missing dependency sqlfluff-dbt-templater and quick fix on source template
- Fixed bug with flatten in different source database

## dbt-coves [0.21.0-a.7] - 2021-10-26

### Features

- Fix bug in templates, columns where lowercased and they should have not

## dbt-coves [0.21.0-a.6] - 2021-10-21

### Features

- [#70](https://github.com/datacoves/dbt-coves/issues/70) Generate sources from tables/views located on a different database
- New dbt-coves setup command that helps set up development environment

## dbt-coves [0.21.0-a.2] - 2021-10-15

### Features

- [#68](https://github.com/datacoves/dbt-coves/issues/68) Support dbt 0.21.0

## dbt-coves [0.20.0-a.3] - 2021-08-16

### Bug Fixes

- [#31](https://github.com/datacoves/dbt-coves/issues/31) Pre-commit rule didn't pass, but shows as passed on CI job

## dbt-coves [0.20.0-a.2] - 2021-08-06

### Features

- [#16](https://github.com/datacoves/dbt-coves/issues/16) Select which schemas to inspect when generating sources, i.e. `dbt-coves generate sources --shemas=RAW_*`.

  Select which relations to inspect as well by running i.e. `dbt-coves generate sources --relations=S*RC_*`.

  Both `schemas` and `relations` selectors can be combined in the same run.

## dbt-coves [0.20.0-a.1] - 2021-07-28

### Bug Fixes

- [#5](https://github.com/datacoves/dbt-coves/issues/5) Generate source throws exception when VARIANT contains no json.

### Features

- [#24](https://github.com/datacoves/dbt-coves/issues/24) When initializing a new dbt project, it's good to create every file in the current folder instead of on a new one.
  By passing the argument --current-dir, the initialization will happen in the current directory.
