# dbt-coves

[![Maintenance](https://img.shields.io/badge/Maintained%3F-yes-green.svg)](https://github.com/datacoves/dbt-coves/graphs/commit-activity)
[![PyPI version
fury.io](https://badge.fury.io/py/dbt-coves.svg)](https://pypi.python.org/pypi/dbt-coves/)
[![Code
Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)
[![Imports:
isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Imports:
python](https://img.shields.io/badge/python-3.8%20%7C%203.9-blue)](https://img.shields.io/badge/python-3.8%20%7C%203.9-blue)
[![Build](https://github.com/datacoves/dbt-coves/actions/workflows/main_ci.yml/badge.svg)](https://github.com/datacoves/dbt-coves/actions/workflows/main_ci.yml/badge.svg)

<!-- [![codecov](https://codecov.io/gh/datacoves/dbt-coves/branch/main/graph/badge.svg?token=JB0E0LZDW1)](https://codecov.io/gh/datacoves/dbt-coves) -->

[![Maintainability](https://api.codeclimate.com/v1/badges/1e6a887de605ef8e0eca/maintainability)](https://codeclimate.com/github/datacoves/dbt-coves/maintainability)
[![Downloads](https://pepy.tech/badge/dbt-coves)](https://pepy.tech/project/dbt-coves)

## What is dbt-coves?

dbt-coves is a CLI tool that automates certain tasks for [dbt](https://www.getdbt.com) making life simpler for the dbt user.

dbt-coves generates dbt soruces and staging models and property(yml) files by analyzing information from the data warehouse and creating the necessary files (sql and yml).

Finally, dbt-coves includes functionality to bootstrap a dbt project and to extract and load configurations from Airbyte.

## Supported dbt versions

| Version | Status           |
| ------- | ---------------- |
| \< 1.0  | ❌ Not supported |
| >= 1.0  | ✅ Tested        |

## Supported adapters

| Feature                           | Snowflake | Redshift       |
| --------------------------------- | --------- | -------------- |
| dbt project setup                 | ✅ Tested | 🕥 In progress |
| source model (sql) generation     | ✅ Tested | 🕥 In progress |
| model properties (yml) generation | ✅ Tested | 🕥 In progress |

NOTE: Other database adapters may work, we have just not tested them. Feed free to try them and let us know if you test them we can update the table above.

### Here\'s the tool in action

[![image](https://cdn.loom.com/sessions/thumbnails/74062cf71cbe4898805ca508ea2d9455-1624905546029-with-play.gif)](https://www.loom.com/share/74062cf71cbe4898805ca508ea2d9455)

# Installation

```console
pip install dbt-coves
```

We recommend using [python
virtualenvs](https://docs.python.org/3/tutorial/venv.html) and create
one separate environment per project.

# Command Reference

For a complete list of options, please run:

```console
dbt-coves -h
dbt-coves <command> -h
```

## Environment setup

Setting up your environment can be done in two different ways:

Runs a set of scripts in your local environment to configure your project components: `ssh keys`, `git` and `dbt`

```console
dbt-coves setup all
```

You can configure individual components:

Set up `git` repository of dbt-coves project

```console
dbt-coves setup git
```

Setup `dbt` within the project (delegates to dbt init)

```console
dbt-coves setup dbt
```

Set up SSH Keys for dbt project. Supports the argument `--open_ssl_public_key` which generates an extra Public Key in Open SSL format, useful for configuring certain providers (i.e. Snowflake authentication)

```console
dbt-coves setup ssh
```

## Models generation

```console
dbt-coves generate <resource>
```

Where _\<resource\>_ could be _sources_, _properties_ or _metadata_.

```console
dbt-coves generate sources
```

This command will generate the dbt source configuration as well as the initial dbt staging model(s). It will look in the database defined in your `profiles.yml` file or you can pass the `--database` argument or set up default configuration options (see below)

```console
dbt-coves generate sources --database raw
```

Supports Jinja templates to adjust how the resources are generated. See below for examples.

### Source Generation Arguments

dbt-coves can be used to create the initial staging models. It will do the following:

1. Create / Update the source yml file
2. Create the initial staging model(sql) file and offer to flatten VARIANT(JSON) fields
3. Create the staging model's property(yml) file.

`dbt-coves generate sources` supports the following args:

See full list in help

```console
dbt-coves generate sources -h
```

```console
--database
# Database to inspect
```

```console
--schema
# Schema to inspect
```

```console
--sources-destination
# Where sources yml files will be generated, default: 'models/staging/{{schema}}/sources.yml'
```

```console
--sources-destination
# Where sources yml files will be generated, default: 'models/staging/{{schema}}/{{schema}}.yml'
```

```console
--models-destination
# Where models sql files will be generated, default: 'models/staging/{{schema}}/{{relation}}.sql'
```

```console
--model-props-destination
# Where models yml files will be generated, default: 'models/staging/{{schema}}/{{relation}}.yml'
```

```console
--update-strategy
# Action to perform when a property file already exists: 'update', 'recreate', 'fail', 'ask' (per file)
```

### Properties Generation Arguments

You can use dbt-coves to generate and update the properties(yml) file for a given dbt model(sql) file.

`dbt-coves generate properties` supports the following args:

```console
--destination
# Where models yml files will be generated, default: '{{model_folder_path}}/{{model_file_name}}.yml'
```

```console
--update-strategy
# Action to perform when a property file already exists: 'update', 'recreate', 'fail', 'ask' (per file)
```

```shell
-s --select
# Filter model(s) to generate property file(s)
```

```shell
--exclude
# Filter model(s) to exclude from property file(s) generation
```

```shell
--selector
# Specify dbt selector for more complex model filtering
```

Note: `--select (or -s)`, `--exclude` and `--selector` work exactly as `dbt ls` selectors do. For usage details, visit [dbt list docs](https://docs.getdbt.com/reference/commands/list)

### Metadata Generation Arguments

You can use dbt-coves to generate the metadata file(s) containing the basic structure of the csv that can be used in the above `dbt-coves generate sources/properties` commands.
Usage of these metadata files can be found in [metadata](https://github.com/datacoves/dbt-coves#metadata) below.

`dbt-coves generate metadata` supports the following args:

```shell
--database
# Database to inspect
```

```shell
--schema
# Schema to inspect
```

```shell
--destination
# Where csv file(s) will be generated, default: 'metadata.csv'
# Supports using the Jinja tags `{{relation}}` and `{{schema}}`
# if creating one csv per relation/table in schema, i.e: "metadata/{{relation}}.csv"
```

### Metadata

dbt-coves supports the argument `--metadata` which allows users to specify a csv file containing field types and descriptions to be used when creating the staging models and property files.

```console
dbt-coves generate sources --metadata metadata.csv
```

Metadata format:
You can download a [sample csv file](sample_metadata.csv) as reference

| database | schema | relation                          | column          | key  | type    | description                                     |
| -------- | ------ | --------------------------------- | --------------- | ---- | ------- | ----------------------------------------------- |
| raw      | raw    | \_airbyte_raw_country_populations | \_airbyte_data  | Year | integer | Year of country population measurement          |
| raw      | raw    | \_airbyte_raw_country_populations | \_airbyte_data  |      | variant | Airbyte data columns (VARIANT) in Snowflake     |
| raw      | raw    | \_airbyte_raw_country_populations | \_airbyte_ab_id |      | varchar | Airbyte unique identifier used during data load |

## Extract configuration from Airbyte

```console
dbt-coves extract airbyte
```

Extracts the configuration from your Airbyte sources, connections and destinations (excluding credentials) and stores it in the specified folder. The main goal of this feature is to keep track of the configuration changes in your git repo, and rollback to a specific version when needed.

Full usage example:

```console
dbt-coves extract airbyte --host http://airbyte-server --port 8001 --path /config/workspace/load
```

## Load configuration to Airbyte

```console
dbt-coves load airbyte
```

Loads the Airbyte configuration generated with `dbt-coves extract airbyte` on an Airbyte server. Secrets folder needs to be specified separately. You can use [git-secret](https://git-secret.io/) to encrypt secrets and make them part of your git repo.

### Loading secrets

Secret credentials can be approached in two different ways: locally or remotely (through a provider/manager).

In order to load encrypted fields locally:

```console
dbt-coves load airbyte --secrets-path /path/to/secret/directory

# This directory must have 'sources', 'destinations' and 'connections' folders nested inside, and inside them the respective JSON files with unencrypted fields.
# Naming convention: JSON unencrypted secret files must be named exactly as the extracted ones.
```

To load encrypted fields through a manager (in this case we are connecting to Datacoves' Service Credentials):

```console
--secrets-manager datacoves
```

```console
--secrets-url https://api.datacoves.localhost/service-credentials/airbyte
```

```console
--secrets-token <secret token>
```

Full usage example:

```console
dbt-coves load airbyte --host http://airbyte-server --port 8001 --path /config/workspace/load --secrets-path /config/workspace/secrets
```

## Run dbt commands

```shell
dbt-coves dbt <arguments> -- <command>
```

Run dbt commands on special environments such as Airflow, or CI workers, with the possibility of changing dbt project location and activating a specific virtual environment in which running commands.

### Arguments

`dbt-coves dbt` supports the following arguments

```shell
--project-dir
# Path of the dbt project where command will be executed, i.e.: /opt/user/dbt_project
```

```shell
--virtualenv
# Virtual environment path. i.e.: /opt/user/virtualenvs/airflow
```

### Sample usage

```shell
dbt-coves dbt --project-dir /opt/user/dbt_project --virtualenv /opt/user/virtualenvs/airflow -- run -s model --vars \"{key: value}\"
# Make sure to escape special characters such as quotation marks
# Double dash (--) between <arguments> and <command> are mandatory
```

# Settings

dbt-coves will read settings from `.dbt_coves/config.yml`. A standard settings files could look like
this:

```yaml
generate:
  sources:
    database: RAW # Database where to look for source tables
    schemas: # List of schema names where to look for source tables
      - RAW
    sources_destination: "models/staging/{{schema}}/{{schema}}.yml" # Where sources yml files will be generated
    models_destination: "models/staging/{{schema}}/{{relation}}.sql" # Where models sql files will be generated
    model_props_destination: "models/staging/{{schema}}/{{relation}}.yml" # Where models yml files will be generated
    update_strategy: ask # Action to perform when a property file already exists. Options: update, recreate, fail, ask (per file)
    templates_folder: ".dbt_coves/templates" # Folder where source generation jinja templates are located. Override default templates creating  source_props.yml, source_model_props.yml, and source_model.sql under this folder

  properties:
    destination: "{{model_folder_path}}/{{model_file_name}}.yml" # Where models yml files will be generated
    # You can specify a different path by declaring it explicitly, i.e.: "models/staging/{{model_file_name}}.yml"
    update-strategy: ask # Action to perform when a property file already exists. Options: update, recreate, fail, ask (per file)
    select: "models/staging/bays" # Filter model(s) to generate property file(s)
    exclude: "models/staging/bays/test_bay" # Filter model(s) to generate property file(s)
    selector: "selectors/bay_selector.yml" # Specify dbt selector for more complex model filtering

  metadata:
    database: RAW # Database where to look for source tables
    schemas: # List of schema names where to look for source tables
      - RAW
    destination: # Where metadata file will be generated, default: 'metadata.csv'

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
    secrets_manager: datacoves # (optional) Secret credentials provider (secrets_path OR secrets_manager should be used, can't load secrets locally and remotely at the same time)
    secrets_path: /config/workspace/secrets # (optional) Secret files location if secrets_manager was not specified
    secrets_url: https://api.datacoves.localhost/service-credentials/airbyte # Secrets url if secrets_manager is datacoves
    secrets_token: <TOKEN> # Secrets auth token if secrets_manager is datacoves
```

## Override generation templates

Customizing generated models and model properties requires placing
template files under the `.dbt-coves/templates` folder.

There are different variables available in the templates:

- `adapter_name` refers to the Adapter's class name being used by the target, e.g. `SnowflakeAdapter` when using [Snowflake](https://github.com/dbt-labs/dbt-snowflake/blob/21b52127e7d221db8b92114aae066fb8a7151bba/dbt/adapters/snowflake/impl.py#L33).
- `columns` contains the list of relation columns that don't contain nested (JSON) data, it's type is `List[Item]`.
- `nested` contains a dict of nested columns, grouped by column name, it's type is `Dict[column_name, Dict[nested_key, Item]]`.

`Item` is a `dict` with the keys `id`, `name`, `type`, and `description`, where `id` contains an slugified id generated from `name`.

### dbt-coves generate sources

#### Source property file (.yml) template

This file is used to create the sources yml file

[source_props.yml](dbt_coves/templates/source_props.yml)

#### Staging model file (.sql) template

This file is used to create the staging model (sql) files.

[staging_model.sql](dbt_coves/templates/staging_model.sql)

#### Staging model property file (.yml) template

This file is used to create the model properties (yml) file

[staging_model_props.yml](dbt_coves/templates/staging_model_props.yml)

### dbt-coves generate properties

This file is used to create the properties (yml) files for models

[model_props.yml](dbt_coves/templates/model_props.yml)

# Thanks

The project main structure was inspired by [dbt-sugar](https://github.com/bitpicky/dbt-sugar). Special thanks to [Bastien Boutonnet](https://github.com/bastienboutonnet) for the great work done.

# Authors

- Sebastian Sassi [\@sebasuy](https://twitter.com/sebasuy) -- [Datacoves](https://datacoves.com/)
- Noel Gomez [\@noel_g](https://twitter.com/noel_g) -- [Datacoves](https://datacoves.com/)
- Bruno Antonellini -- [Datacoves](https://datacoves.com/)

# About

Learn more about [Datacoves](https://datacoves.com).

⚠️ **dbt-coves is still in development, make sure to test it for your dbt project version and DW before using in production and please submit any issues you find. We also welcome any contributions from the community**
