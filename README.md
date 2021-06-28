# dbt-coves

[![PyPI version](https://badge.fury.io/py/dbt-coves.svg)](https://badge.fury.io/py/dbt-coves)

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Checked with mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org/)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
![python](https://img.shields.io/badge/python-3.8%20%7C%203.9-blue)

![Build](https://github.com/datacoves/dbt-coves/actions/workflows/main_ci.yml/badge.svg)
[![pre-commit.ci status](https://results.pre-commit.ci/badge/github/bitpicky/dbt-coves/main.svg)](https://results.pre-commit.ci/latest/github/datacoves/dbt-coves/main)
[![codecov](https://codecov.io/gh/datacoves/dbt-coves/branch/main/graph/badge.svg?token=JB0E0LZDW1)](https://codecov.io/gh/datacoves/dbt-coves)
[![Maintainability](https://api.codeclimate.com/v1/badges/1e6a887de605ef8e0eca/maintainability)](https://codeclimate.com/github/datacoves/dbt-coves/maintainability)

[![Downloads](https://pepy.tech/badge/dbt-coves)](https://pepy.tech/project/dbt-coves)

## What is dbt-coves?

dbt-coves is a complimentary CLI tool for [dbt](https://www.getdbt.com/) that allows users to quickly apply [Analytics Engineering](https://www.getdbt.com/what-is-analytics-engineering/) best practices.

:warning: **dbt-coves is in alpha version, don't use on your prod models unless you have tested it in a safe place before.**

### Supported dbt versions

| Version | Status | 
|---------|--------|
| 0.17.0  | :clock1030: In progress |
| 0.18.2  | :clock1030: In progress  |
| 0.19.1  | :white_check_mark: Tested |
| 0.20.0  | :x: Not tested  |

### Supported adapters

| Feature | Snowflake | Redshift | BigQuery | Postgres |
|---------|--------|--|--|--|
| profile.yml generation | :white_check_mark: Tested | :x: Not tested  | :x: Not tested  | :x: Not tested  |
| sources generation | :white_check_mark: Tested | :x: Not tested  | :x: Not tested  | :x: Not tested  |

### Installation

```
pip install dbt-coves
```

### Main features

#### Project initialization

```
dbt-coves init
```

Initializes a new ready-to-use dbt project that includes recommended integrations such as [sqlfluff](https://github.com/sqlfluff/sqlfluff), [pre-commit](https://pre-commit.com/), dbt packages, among others.

Uses a [cookiecutter](https://github.com/datacoves/cookiecutter-dbt) template to make it easier to maintain.

#### Models generation

```
dbt-coves generate <resource>
```

Where `<resource>` could be `sources`.

Code generation tool to easily generate models and model properties based on configuration and existing data.

Supports [Jinja](https://jinja.palletsprojects.com/) templates to adjust how the resources are generated.

#### Quality Assurance

```
dbt-coves check
```

Runs a set of checks in your local environment to ensure high quality data.

Checks can be extended by implementing [pre-commit hooks](https://pre-commit.com/#creating-new-hooks).

## Thanks

The project main structure was inspired by [dbt-sugar](https://github.com/bitpicky/dbt-sugar). Special thanks to [Bastien Boutonnet](https://github.com/bastienboutonnet) for the great work done.

## Authors

- Sebastian Sassi ([@sebasuy](https://twitter.com/sebasuy)) – [Convexa](https://convexa.ai)
- Noel Gomez ([@noel_g](https://twitter.com/noel_g)) – [Ninecoves](https://ninecoves.com)

## About

Learn more about [Datacoves](https://datacoves.com).
