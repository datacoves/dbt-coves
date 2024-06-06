### Metadata Generation Arguments

You can use dbt-coves to generate the metadata file(s) containing the basic structure of the csv that can be used in the above `dbt-coves generate sources/properties` commands.
Usage of these metadata files can be found in [metadata](https://github.com/datacoves/dbt-coves#metadata) below.

`dbt-coves generate metadata` supports the following args:

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
--destination
# Where csv file(s) will be generated, default: 'metadata.csv'
# Supports using the Jinja tags `{{relation}}` and `{{schema}}`
# if creating one csv per relation/table in schema, i.e: "metadata/{{relation}}.csv"
```

```console
--no-prompt
# Silently generate metadata
```
