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

⚠️ **dbt-coves is in alpha, make sure to test it for your dbt project version and DW before using in production**

### Here\'s the tool in action

[![image](https://cdn.loom.com/sessions/thumbnails/74062cf71cbe4898805ca508ea2d9455-1624905546029-with-play.gif)](https://www.loom.com/share/74062cf71cbe4898805ca508ea2d9455)

## Supported dbt versions

  |Version          |Status|
  |---------------- |------------------|
  |\< 1.0       |❌ Not supported|
  |>= 1.0            |✅ Tested|

## Supported adapters

  |Feature|                  Snowflake|   Redshift|         BigQuery|       
  |------------------------| -----------| ----------------| ---------------|
  |dbt project setup|   ✅ Tested|   🕥 In progress|   ❌ Not tested|  
  |source model (sql) generation|       ✅ Tested|   🕥 In progress|   ❌ Not tested|  
  |model properties (yml) generation|       ✅ Tested|   🕥 In progress|   ❌ Not tested|  

# Installation

``` console
pip install dbt-coves
```

We recommend using [python
virtualenvs](https://docs.python.org/3/tutorial/venv.html) and create
one separate environment per project.

# Main Features

For a complete detail of usage, please run:

``` console
dbt-coves -h
dbt-coves <command> -h
```

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

Where *\<resource\>* could be *sources* or *properties*.

Code generation tool to easily generate models and model properties
based on configuration and existing data.

Supports [Jinja](https://jinja.palletsprojects.com/) templates to adjust
how the resources are generated.

### Arguments

```console
--sources-destination
# Where sources yml files will be generated, i.e. 'models/staging/{{schema}}/sources.yml'
```

```console
--models-destination
# Where models sql files will be generated, i.e 'models/staging/{{schema}}/{{relation}}.sql'
```

```console
--model-props-destination
# Where models yml files will be generated, i.e. 'models/staging/{{schema}}/{{relation}}.yml'
```

```console
--update-strategy
# Action to perform when a property file already exists: 'update', 'recreate', 'fail', 'ask' (per file)
```

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
  
## Environment setup

Setting up your environment can be done in two different ways:

``` console
dbt-coves setup all
```

Runs a set of checks in your local environment and helps you configure every project component properly: ssh key, git, dbt profiles.yml, vscode extensions, sqlfluff and precommit.

You can also configure individual components:

``` console
dbt-coves setup git
```
Set up Git repository of dbt-coves project


``` console
dbt-coves setup dbt
```
Setup `dbt` within the project (delegates to dbt init)


``` console
dbt-coves setup ssh
```
Set up SSH Keys for dbt-coves project. Supports the argument `--open_ssl_public_key` which generates an extra Public Key in Open SSL format, useful for configuring certain providers (i.e. Snowflake authentication)

``` console
dbt-coves setup vscode
```
Setup of predefined `settings.json` for `vscode`, `settings.json` may be added to .dbt_coves/templates/ folder

``` console
dbt-coves setup sqlfluff
```
Set up `sqlfluff` of dbt-coves project. Supports `--templates` argument for using your custom `.sqlfluff` configuration file

``` console
dbt-coves setup precommit
```
Setup of default `pre-commit` template of dbt-coves project. Supports `--templates` argument for using your custom `.pre-commit-config.yaml` configuration file

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
        {{ '"' + col['name'] + '"' }} as {{ col['id'] }}{% if not loop.last %},{% endif %}
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
      - name: {{ col['id'] }}
      {%- if col['description'] %}
        description: "{{ col['description'] }}"
      {%- endif %}
{%- endfor %}

```

### model_props.yml
```yaml
version: 2

models:
  - name: {{ model.lower() }}
    columns:
{%- for col in columns %}
      - name: {{ col['id'] }}
      {%- if col['description'] %}
        description: "{{ col['description'] }}"
      {%- endif %}
{%- endfor %}

```

### model.yml
```yaml
version: 2

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
    [Datacoves](https://datacoves.com/)
-   Noel Gomez [\@noel_g](https://twitter.com/noel_g) --
    [Datacoves](https://datacoves.com/)
-   Bruno Antonellini --
    [Datacoves](https://datacoves.com/)

# About

Learn more about [Datacoves](https://datacoves.com).


