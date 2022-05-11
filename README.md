# dbt-coves

[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/datacoves/dbt-coves/graphs/commit-activity)
[![PyPI version
fury.io](https://badge.fury.io/py/dbt-coves.svg)](https://pypi.python.org/pypi/dbt-coves/)
[![Code
Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Checked with
mypy](http://www.mypy-lang.org/static/mypy_badge.svg)](http://mypy-lang.org)
[![Imports:
isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Imports:
python](https://img.shields.io/badge/python-3.8%20%7C%203.9-blue)](https://img.shields.io/badge/python-3.8%20%7C%203.9-blue)
[![Build](https://github.com/datacoves/dbt-coves/actions/workflows/main_ci.yml/badge.svg)](https://github.com/datacoves/dbt-coves/actions/workflows/main_ci.yml/badge.svg)
[![pre-commit.ci
status](https://results.pre-commit.ci/badge/github/bitpicky/dbt-coves/main.svg)](https://results.pre-commit.ci/latest/github/datacoves/dbt-coves/main)
[![codecov](https://codecov.io/gh/datacoves/dbt-coves/branch/main/graph/badge.svg?token=JB0E0LZDW1)](https://codecov.io/gh/datacoves/dbt-coves)
[![Maintainability](https://api.codeclimate.com/v1/badges/1e6a887de605ef8e0eca/maintainability)](https://codeclimate.com/github/datacoves/dbt-coves/maintainability)
[![Downloads](https://pepy.tech/badge/dbt-coves)](https://pepy.tech/project/dbt-coves)

## What is dbt-coves?

dbt-coves is a complimentary CLI tool for [dbt](https://www.getdbt.com)
that allows users to quickly apply [Analytics
Engineering](https://www.getdbt.com/what-is-analytics-engineering/) best
practices.

dbt-coves helps with the generation of scaffold for dbt by analyzing
your data warehouse schema in Redshift, Snowflake, or Big Query and
creating the necessary configuration files (sql and yml).

⚠️ **dbt-coves is in alpha version. Don't use on your prod models unless
you have tested it before.**

### Here\'s the tool in action

[![image](https://cdn.loom.com/sessions/thumbnails/74062cf71cbe4898805ca508ea2d9455-1624905546029-with-play.gif)](https://www.loom.com/share/74062cf71cbe4898805ca508ea2d9455)

## Supported dbt versions

  |Version          |Status|
  |---------------- |------------------|
  |\<= 0.17.0       |❌ Not supported|
  |0.18.x - 0.21x   |✅ Tested|
  |1.x              |✅ Tested|

## Supported adapters

  |Feature|                  Snowflake|   Redshift|         BigQuery|        Postgres|
  |------------------------| -----------| ----------------| ---------------| ---------------|
  |profile.yml generation|   ✅ Tested|   🕥 In progress|   ❌ Not tested|   ❌ Not tested|
  |sources generation|       ✅ Tested|   🕥 In progress|   ❌ Not tested|   ❌ Not tested|

# Installation

``` console
pip install dbt-coves
```

We recommend using [python
virtualenvs](https://docs.python.org/3/tutorial/venv.html) and create
one separate environment per project.

⚠️ **if you have dbt \< 0.18.0 installed, dbt-coves will automatically
upgrade dbt to the latest version**

# Main Features

## Project initialization

``` console
dbt-coves init
```

Initializes a new ready-to-use dbt project that includes recommended
integrations such as [sqlfluff](https://github.com/sqlfluff/sqlfluff),
[pre-commit](https://pre-commit.com/), dbt packages, among others.

Uses a [cookiecutter](https://github.com/datacoves/cookiecutter-dbt)
template to make it easier to maintain.

## Models generation

``` console
dbt-coves generate <resource>
```

Where *\<resource\>* could be *sources*.

Code generation tool to easily generate models and model properties
based on configuration and existing data.

Supports [Jinja](https://jinja.palletsprojects.com/) templates to adjust
how the resources are generated.

### Metadata

Supports the argument *--metadata* which allows to specify a csv file
containing field types and descriptions to be inserted into the model
property files.

``` console
dbt-coves generate sources --metadata metadata.csv
```

Metadata format:

  
  |database|   schema|     relation|   column|     key|         type|       description|
  |----------| ----------| ----------| ----------| -----------| ----------| -------------|
  |raw|        master|     person|     name|       (empty)|     varchar|    The full name|
  |raw|        master|     person|     name|       groupName|   varchar|    The group name|
  

## Quality Assurance

``` console
dbt-coves check
```

Runs a set of checks in your local environment to ensure high code
quality.

Checks can be extended by implementing [pre-commit
hooks](https://pre-commit.com/#creating-new-hooks).

## Environment setup

``` console
dbt-coves setup
```

Runs a set of checks in your local environment and helps you configure
it properly: ssh key, git, dbt profiles.yml, vscode extensions.

## Extract configuration from Airbyte

``` console
dbt-coves extract airbyte
```

Extracts the configuration from your Airbyte sources, connections and
destinations (excluding credentials) and stores it in the specified
folder. The main goal of this feature is to keep track of the
configuration changes in your git repo, and rollback to a specific
version when needed.

## Load configuration to Airbyte

``` console
dbt-coves load airbyte
```

Loads the Airbyte configuration generated with *dbt-coves extract
airbyte* on an Airbyte server. Secrets folder needs to be specified
separatedly. You can use [git-secret](https://git-secret.io/) to encrypt
them and make them part of your git repo.

# Settings

Dbt-coves could optionally read settings from `.dbt_coves.yml` or
`.dbt_coves/config.yml`. A standard settings files could looke like
this:

``` yaml
generate:
  sources:
    schemas:
      - RAW
    destination: "models/sources/{{ schema }}/{{ relation }}.sql"
    model_props_strategy: one_file_per_model
    templates_folder: ".dbt_coves/templates"
```

In this example options for the `generate` command are provided:

`schemas`: List of schema names where to look for source tables

`destination`: Path to generated model, where `schema` represents the
lowercased schema and `relation` the lowercased table name.

`model_props_strategy`: Defines how dbt-coves generates model properties
files, currently just `one_file_per_model` is available, creates one
yaml file per model.

`templates_folder`: Folder where source generation jinja templates are
located.

## Override source generation templates

Customizing generated models and model properties requires placing
specific files under the `templates_folder` folder like these:

### source_model.sql

``` sql
with raw_source as (

    select
        *
    from {% raw %}{{{% endraw %} source('{{ relation.schema.lower() }}', '{{ relation.name.lower() }}') {% raw %}}}{% endraw %}

),

final as (

    select
{%- if adapter_name == 'SnowflakeAdapter' %}
{%- for key, cols in nested.items() %}
  {%- for col in cols %}
        {{ key }}:{{ '"' + col + '"' }}::{{ cols[col]["type"] }} as {{ cols[col]["id"] }}{% if not loop.last or columns %},{% endif %}
  {%- endfor %}
{%- endfor %}
{%- elif adapter_name == 'BigQueryAdapter' %}
{%- for key, cols in nested.items() %}
  {%- for col in cols %}
        cast({{ key }}.{{ col }} as {{ cols[col]["type"].replace("varchar", "string") }}) as {{ cols[col]["id"] }}{% if not loop.last or columns %},{% endif %}
  {%- endfor %}
{%- endfor %}
{%- elif adapter_name == 'RedshiftAdapter' %}
{%- for key, cols in nested.items() %}
  {%- for col in cols %}
        {{ key }}.{{ col }}::{{ cols[col]["type"] }} as {{ cols[col]["id"] }}{% if not loop.last or columns %},{% endif %}
  {%- endfor %}
{%- endfor %}
{%- endif %}
{%- for col in columns %}
        {{ '"' + col.name + '"' }} as {{ col.name.lower() }}{% if not loop.last %},{% endif %}
{%- endfor %}

    from raw_source

)

select * from final
```

### source_model_props.yml

``` yaml
version: 2

sources:
  - name: {{ relation.schema.lower() }}
{%- if source_database %}
    database: {{ source_database }}
{%- endif %}
    schema: {{ relation.schema.lower() }}
    tables:
      - name: {{ relation.name.lower() }}
        identifier: {{ relation.name }}

models:
  - name: {{ model.lower() }}
    columns:
{%- for cols in nested.values() %}
  {%- for col in cols %}
      - name: {{ cols[col]["id"] }}
      {%- if cols[col]["description"] %}
        description: "{{ cols[col]['description'] }}"
      {%- endif %}
  {%- endfor %}
{%- endfor %}
{%- for col in columns %}
      - name: {{ col.name.lower() }}
{%- endfor %}
```

# Thanks

The project main structure was inspired by
[dbt-sugar](https://github.com/bitpicky/dbt-sugar). Special thanks to
[Bastien Boutonnet](https://github.com/bastienboutonnet) for the great
work done.

# Authors

-   Sebastian Sassi [\@sebasuy](https://twitter.com/sebasuy) --
    [Convexa](https://convexa.ai)
-   Noel Gomez [\@noel_g](https://twitter.com/noel_g) --
    [Ninecoves](https://ninecoves.com)

# About

Learn more about [Datacoves](https://datacoves.com).

# CLI Reference

For a complete detail of usage, please run:

``` console
dbt-coves -h
```
