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

## Environment setup

Setting up your environment can be done in two different ways:

``` console
dbt-coves setup all
```

Runs a set of checks in your local environment and helps you configure every project component properly: `ssh keys`, `git` and `dbt` 

You can also configure individual components:

``` console
dbt-coves setup git
```
Set up `git` repository of dbt-coves project


``` console
dbt-coves setup dbt
```
Setup `dbt` within the project (delegates to dbt init)


``` console
dbt-coves setup ssh
```
Set up SSH Keys for dbt-coves project. Supports the argument `--open_ssl_public_key` which generates an extra Public Key in Open SSL format, useful for configuring certain providers (i.e. Snowflake authentication)

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

`dbt-coves generate sources` supports the following args:

```shell
--sources-destination
# Where sources yml files will be generated, default: 'models/staging/{{schema}}/sources.yml'
```

```shell
--models-destination
# Where models sql files will be generated, default: 'models/staging/{{schema}}/{{relation}}.sql'
```

```shell
--model-props-destination
# Where models yml files will be generated, default: 'models/staging/{{schema}}/{{relation}}.yml'
```

```shell
--update-strategy
# Action to perform when a property file already exists: 'update', 'recreate', 'fail', 'ask' (per file)
```

`dbt-coves generate properties` supports the following args:

```shell
--destination
# Where models yml files will be generated, default: '{{model_folder_path}}/{{model_file_name}}.yml'
```

```shell
--update-strategy
# Action to perform when a property file already exists: 'update', 'recreate', 'fail', 'ask' (per file)
```

```shell
--model
# Model(s) path where 'dbt ls' will look for models for generation, i.e: 'models/staging' or 'models/staging/my_model.sql'
```

### Metadata

Supports the argument *--metadata* which allows to specify a csv file
containing field types and descriptions to be inserted into the model
property files.

``` shell
dbt-coves generate sources --metadata metadata.csv
```

Metadata format:

  
  |database|   schema|     relation|   column|     key|         type|       description|
  |----------| ----------| ----------| ----------| -----------| ----------| -------------|
  |raw|        master|     person|     name|       (empty)|     varchar|    The full name|
  |raw|        master|     person|     name|       groupName|   varchar|    The group name|
  


## Extract configuration from Airbyte

``` shell
dbt-coves extract airbyte
```

Extracts the configuration from your Airbyte sources, connections and
destinations (excluding credentials) and stores it in the specified
folder. The main goal of this feature is to keep track of the
configuration changes in your git repo, and rollback to a specific
version when needed.

Full usage example:
```shell
dbt-coves extract airbyte --host http://airbyte-server --port 8001 --path /config/workspace/load
```
## Load configuration to Airbyte

``` shell
dbt-coves load airbyte
```

Loads the Airbyte configuration generated with *dbt-coves extract
airbyte* on an Airbyte server. Secrets folder needs to be specified
separatedly. You can use [git-secret](https://git-secret.io/) to encrypt
them and make them part of your git repo.

### Loading secrets

Secret credentials can be approached in two different ways: locally or remotely (through a provider/manager).

In order to load encrypted fields locally:

```shell
dbt-coves load airbyte --secrets-path /path/to/secret/directory

# This directory must have 'sources', 'destinations' and 'connections' folders nested inside, and inside them the respective JSON files with unencrypted fields.
# Naming convention: JSON unencrypted secret files must be named exactly as the extracted ones.
```

To load encrypted fields through a manager (in this case we are connecting to Datacoves' Service Credentials):

```shell
--secrets-manager datacoves
```

```shell
--secrets-url https://api.datacoves.localhost/service-credentials/airbyte
```

```shell
--secrets-token AbCdEf123456
```

Full usage example:
```shell
dbt-coves load airbyte --host http://airbyte-server --port 8001 --path /config/workspace/load --secrets-path /config/workspace/secrets
```

# Settings

Dbt-coves could optionally read settings from `.dbt_coves.yml` or
`.dbt_coves/config.yml`. A standard settings files could looke like
this:

``` yaml
generate:
  sources:
    database: RAW # Database where to look for source tables
    schemas: # List of schema names where to look for source tables
      - RAW
    sources_destination: "models/staging/{{schema}}/sources.yml" # Where sources yml files will be generated
    models_destination: "models/staging/{{schema}}/{{relation}}.sql" # Where models sql files will be generated
    model_props_destination: "models/staging/{{schema}}/{{relation}}.yml" # Where models yml files will be generated
    update_strategy: ask # Action to perform when a property file already exists. Options: update, recreate, fail, ask (per file)
    templates_folder: ".dbt_coves/templates" # Folder where source generation jinja templates are located. Override default templates creating source_model_props.yml, source_props.yml and source_model.sql under this folder

  properties:
    destination: "{{model_folder_path}}/{{model_file_name}}.yml" # Where models yml files will be generated
    # You can specify a different path by declaring it explicitly, i.e.: "models/staging/{{model_file_name}}.yml"
    update-strategy: ask # Action to perform when a property file already exists. Options: update, recreate, fail, ask (per file)
    models: "models/staging" # Model(s) path where 'generate properties' will look for models for generation

extract:
  airbyte:
    path: /config/workspace/load # Where json files will be generated
    host: http://airbyte-server # Airbyte's API hostname
    port: 8001 # Airbyte's API port
    dbt_list_args: --exclude source:dbt_artifacts # Extra dbt arguments: selectors, modifiers, etc

load:
  airbyte:
    path: /config/workspace/load
    host: http://airbyte-server
    port: 8001
    dbt_list_args: --exclude source:dbt_artifacts
    secrets_path: /config/workspace/secrets # Secret files location for Airbyte configuration
    secrets_manager: datacoves # Secret credentials provider (secrets_path OR secrets_manager should be used, can't load secrets locally and remotely at the same time)
    secrets_url: https://api.datacoves.localhost/service-credentials/airbyte # Secret credentials provider url
    secrets_token: AbCdEf123456 # Secret credentials provider token

```


## Override source generation templates

Customizing generated models and model properties requires placing
specific files under the `templates_folder` folder like these:

### source_model.sql

``` sql
with raw_source as (

    select *
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

### source_props.yml

```yaml
version: 2

sources:
  - name: {{ relation.schema.lower() }}
{%- if source_database %}
    database: {{ source_database }}
{%- endif %}
    tables:
      - name: {{ relation.name.lower() }}

```

### source_model_props.yml

``` yaml
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


