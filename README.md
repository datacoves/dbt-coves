# dbt-coves

## Sponsor

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="images/datacoves-dark.png">
  <img alt="Datacoves" src="images/datacoves-light.png" width="150">
</picture>

Hosted VS Code, dbt-core, SqlFluff, and Airflow, find out more at [Datacoves.com](https://datacoves.com/product).

## Table of contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)
- [Acknowledgements](#acknowledgements)
- [Contributing](#contributing)
- [License](#license)

## Introduction

dbt-coves is a CLI tool that automates certain tasks for [dbt](https://www.getdbt.com), making life simpler for the dbt user.

dbt-coves generates dbt sources, staging models and property(yml) files by analyzing information from the data warehouse and creating the necessary files (sql and yml). It can even generate Airflow DAGs based on YML input.

Finally, dbt-coves includes functionality to bootstrap a dbt project and to extract and load configurations from data-replication providers.

## Installation

```console
pip install dbt-coves
```

We recommend using [python
virtualenvs](https://docs.python.org/3/tutorial/venv.html) and create
one separate environment per project.

#### Supported dbt versions

| Version | Status           |
| ------- | ---------------- |
| \< 1.0  | ❌ Not supported |
| >= 1.0  | ✅ Tested        |

From `dbt-coves` 1.4.0 onwards, our major and minor versions match those of [dbt-core](https://github.com/dbt-labs/dbt-core).
This means we release a new major/minor version once it's dbt-core equivalent is tested.
Patch suffix (1.4.X) is exclusive to our continuous development and does not reflect a version match with dbt.

#### Supported dbt adapters

| Feature                           | Snowflake | Redshift  | BigQuery  |
| --------------------------------- | --------- | --------- | --------- |
| source model (sql) generation     | ✅ Tested | ✅ Tested | ✅ Tested |
| model properties (yml) generation | ✅ Tested | ✅ Tested | ✅ Tested |

NOTE: Other database adapters may work, although we have not tested them. Feel free to try them and let us know so we can update the table above.

## Usage

Thanks to dbt-coves, any analyst or developer can:

- [generate](docs/commands/generate/):
  - [dbt sources](docs/commands/generate/sources/): generate the dbt source configuration as well as the initial dbt staging model(s).
  - [dbt properties](docs/commands/generate/properties/): generate and/or update the properties(yml) file for a given dbt model(sql) file.
  - [dbt docs](docs/commands/generate/docs/): generate dbt docs with extra functionalities.
  - [airflow dags](docs/commands/generate/airflow-dags/): generate Airflow DAGs from YML files.
- [extract and load](docs/commands/extract%20and%20load/): save and restore your data replication provider configuration
  - [Airbyte](docs/commands/extract%20and%20load/airbyte)
  - [Fivetran](docs/commands/extract%20and%20load/fivetran)
- [dbt](docs/commands/dbt/): run dbt commands in special environments.
- [setup](docs/commands/setup/): configure different components of your dbt ETL project.

For a complete list of options, run:

```console
dbt-coves -h
dbt-coves <command> -h
```

## Contributing

If you're interested in contributing to the development of dbt-coves, please refer to the [Contributing Guidelines](contributing.md). This document outlines the process for submitting bug reports, feature requests, and code contributions.

## Acknowledgements

### Thanks

The project main structure was inspired by [dbt-sugar](https://github.com/bitpicky/dbt-sugar). Special thanks to [Bastien Boutonnet](https://github.com/bastienboutonnet) for the great work done.

### Authors

- Sebastian Sassi [\@sebasuy](https://twitter.com/sebasuy) -- [Datacoves](https://datacoves.com/)
- Noel Gomez [\@noel_g](https://twitter.com/noel_g) -- [Datacoves](https://datacoves.com/)
- Bruno Antonellini -- [Datacoves](https://datacoves.com/)

# Metrics

[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/datacoves/dbt-coves/graphs/commit-activity)
[![PyPI version
fury.io](https://badge.fury.io/py/dbt-coves.svg)](https://pypi.python.org/pypi/dbt-coves/)
[![Code
Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Imports:
python](https://img.shields.io/badge/python-3.8%20%7C%203.9-blue)](https://img.shields.io/badge/python-3.8%20%7C%203.9-blue)
[![Build](https://github.com/datacoves/dbt-coves/actions/workflows/main_ci.yml/badge.svg)](https://github.com/datacoves/dbt-coves/actions/workflows/main_ci.yml/badge.svg)

<!-- [![codecov](https://codecov.io/gh/datacoves/dbt-coves/branch/main/graph/badge.svg?token=JB0E0LZDW1)](https://codecov.io/gh/datacoves/dbt-coves) -->

[![Maintainability](https://api.codeclimate.com/v1/badges/1e6a887de605ef8e0eca/maintainability)](https://codeclimate.com/github/datacoves/dbt-coves/maintainability)
[![Downloads](https://pepy.tech/badge/dbt-coves)](https://pepy.tech/project/dbt-coves)
