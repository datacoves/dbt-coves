## dbt-coves generate properties

You can use dbt-coves to generate and update the properties(yml) file for a given dbt model(sql) file.

### Arguments

`dbt-coves generate properties` supports the following args:

```console
--destination
# Where models yml files will be generated, default: '{{model_folder_path}}/{{model_file_name}}.yml'
```

```console
--update-strategy
# Action to perform when a property file already exists: 'update', 'recreate', 'fail', 'ask' (per file)
```

```console
-s --select
# Filter model(s) to generate property file(s)
```

```console
--exclude
# Filter model(s) to exclude from property file(s) generation
```

```console
--selector
# Specify dbt selector for more complex model filtering
```

```console
--templates-folder
# Folder with jinja templates that override default properties generation templates, i.e. 'templates'
```

```console
--metadata
# Path to csv file containing metadata, i.e. 'metadata.csv'
```

```console
--no-prompt
# Silently generate dbt models property files
```

Note: `--select (or -s)`, `--exclude` and `--selector` work exactly as `dbt ls` selectors do. For usage details, visit [dbt list docs](https://docs.getdbt.com/reference/commands/list)

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
