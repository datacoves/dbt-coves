# dbt-coves

[![PyPI version fury.io](https://badge.fury.io/py/dbt-coves.svg)](https://pypi.python.org/pypi/dbt-coves/) ![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dbt_coves) [![Build](https://github.com/datacoves/dbt-coves/actions/workflows/main_ci.yml/badge.svg)](https://github.com/datacoves/dbt-coves/actions/workflows/main_ci.yml/badge.svg)

## Brought to you by Datacoves

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="images/datacoves-dark.png">
  <img alt="Datacoves" src="images/datacoves-light.png" width="150">
</picture>

dbt-coves automates the tedious parts of dbt development. Datacoves takes it further: a managed DataOps platform for dbt and Airflow, deployable in your private cloud or available as SaaS.

- **Private cloud or SaaS** – your data, your choice
- **Managed dbt + Airflow** – production-ready from day one
- **In-browser VS Code** – onboard developers in minutes
- **Bring your own tools** – integrates with your existing stack, no lock-in
- **AI-assisted development** – connect your organization's approved LLM (Anthropic, OpenAI, Azure, Gemini, and more)
- **Built-in governance** – CI/CD, guardrails, and best practices included

dbt-coves is the power tools. Datacoves is the workshop.

[Explore the platform →](https://datacoves.com)

## Overview

[![image](https://cdn.loom.com/sessions/thumbnails/7d5341f5d5b149ed8895fe1187e338c5-with-play.gif)](https://www.loom.com/share/7d5341f5d5b149ed8895fe1187e338c5)

## Table of contents

- [Introduction](#introduction)
- [Installation](#installation)
- [Usage](#usage)
- [Contributing](#contributing)

## Introduction

dbt-coves is a CLI tool that automates and simplifies development and release tasks for [dbt](https://www.getdbt.com).

In addition to other functions listed below, dbt-coves generates dbt sources, staging models and property(yml) files by analyzing information from the data warehouse and creating the necessary files (sql and yml). It can even generate Airflow DAGs based on YML input.

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

**NOTE:** Other database adapters may work, although we have not tested them. Feel free to try them and let us know so we can update the table above.

## Usage

dbt-coves, supports the following functions:

- [dbt](docs/commands/dbt/): run dbt commands in CI and Airflow environments.
- [extract and load](docs/commands/extract%20and%20load/): save and restore your configuration from:
  - [Airbyte](docs/commands/extract%20and%20load/airbyte)
  - [Fivetran](docs/commands/extract%20and%20load/fivetran)
- [generate](docs/commands/generate/):
  - [airflow dags](docs/commands/generate/airflow%20dags/): generate Airflow DAGs from YML files.
  - [dbt docs](docs/commands/generate/docs/): generate dbt docs by merging production catalog.json, useful in combination with [dbt-checkpoint](https://github.com/dbt-checkpoint/dbt-checkpoint) and when using Slim CI
  - [dbt sources](docs/commands/generate/sources/): generate the dbt source configuration as well as the initial dbt staging model(s) and their corresponding property(yml) files.
  - [dbt properties](docs/commands/generate/properties/): generate and/or update the properties(yml) file for a given dbt model(sql) file.
  - [metadata](docs/commands/generate/metadata/): generate metadata extract(CSV file) that can be used to collect column types and descriptions and then provided as input inthe the `generate sources` or `generate properties` command
  - [templates](docs/commands/generate/templates/): generate the dbt-coves templates that dbt-coves utilizes with other dbt-coves commands
- [setup](docs/commands/setup/): used configure different components of a dbt project.

For a complete list of options, run:

```console
dbt-coves -h
dbt-coves <command> -h
```

## Contributing

If you're interested in contributing to the development of dbt-coves, please refer to the [Contributing Guidelines](contributing.md). This document outlines the process for submitting bug reports, feature requests, and code contributions.

# Metrics

[![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/datacoves/dbt-coves/graphs/commit-activity)
[![Maintainability](https://api.codeclimate.com/v1/badges/1e6a887de605ef8e0eca/maintainability)](https://codeclimate.com/github/datacoves/dbt-coves/maintainability)
[![Downloads](https://pepy.tech/badge/dbt-coves)](https://pepy.tech/project/dbt-coves)
