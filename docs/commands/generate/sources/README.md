## dbt-coves generate sources

### Overview

[![image](https://cdn.loom.com/sessions/thumbnails/28857aab6f13462c9cf8561d2ac982fc-with-play.gif)](https://www.loom.com/share/28857aab6f13462c9cf8561d2ac982fc?sid=02d6794d-d9cb-4975-be6e-8731810ebbe0)

This command will generate the dbt source configuration as well as the initial dbt staging model(s). It will look in the database defined in your `profiles.yml` file or you can pass the `--database` argument or set up default configuration options (see below)

```console
dbt-coves generate sources --database raw
```

Supports Jinja templates to adjust how the resources are generated. See below for examples.

dbt-coves can be used to create the initial staging models. It will do the following:

1. Create / Update the source yml file
2. Create the initial staging model(sql) file and offer to flatten VARIANT(JSON) fields
3. Create the staging model's property(yml) file.

### Arguments

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
--schemas
# Schema(s) to inspect. Accepts wildcards (must be enclosed in quotes if used)
```

```console
--select-relations
# List of relations where raw data resides. The parameter must be enclosed in quotes. Accepts wildcards.
```

```console
--exclude-relations
# Filter relation(s) to exclude from source file(s) generation. The parameter must be enclosed in quotes. Accepts wildcards.
```

```console
--sources-destination
# Where sources yml files will be generated, default: 'models/staging/{{schema}}/sources.yml'
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

```console
--templates-folder
# Folder with jinja templates that override default sources generation templates, i.e. 'templates'
```

```console
--metadata
# Path to csv file containing metadata, i.e. 'metadata.csv'
```

```console
--flatten-json-fields
# Action to perform when JSON fields exist: 'yes', 'no', 'ask' (per file)
```

```console
--overwrite-staging-models
# Flag: overwrite existent staging (SQL) files
```

```console
--skip-model-props
# Flag: don't create model's property (yml) files
```

```console
--no-prompt
# Silently generate source dbt models
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
