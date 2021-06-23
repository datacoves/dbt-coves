# dbt-coves

## What is dbt-coves?

dbt-coves is a complimentary CLI tool for [dbt](https://www.getdbt.com/) that allows users to quickly apply [Analytics Engineering](https://www.getdbt.com/what-is-analytics-engineering/) best practices.

### Main features

#### Project initialization

```
dbt-coves init
```

Initializes a new ready-to-use dbt project that includes recommended integrations such as [sqlfluff](https://github.com/sqlfluff/sqlfluff), [pre-commit](https://pre-commit.com/), dbt packages, among others.

Uses [cookiecutter](https://github.com/cookiecutter/cookiecutter) templates to make it easier to maintain.

#### Models generation

```
dbt-coves generate <resource>
```

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
