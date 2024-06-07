## dbt-coves generate metadata

This command will generate a `dbt-coves metadata` file from your database table(s).

`Metadata` consists of comma-separated values in which the user can specify column(s) keys and descriptions

It is particularly useful for providing descriptions to your YML schema files at [generate sources](../sources/README.md#metadata) or [generate properties](../properties/README.md#metadata) time.

### Arguments

`dbt-coves generate metadata` supports the following args:

```console
--database DATABASE
# Database where source relations live, if different than target
```

```console
--schemas SCHEMAS
# Comma separated list of schemas where raw data resides, i.e. 'RAW_SALESFORCE,RAW_HUBSPOT'
```

```console
--select-relations SELECT_RELATIONS
# Comma separated list of relations where raw data resides, i.e. 'RAW_HUBSPOT_PRODUCTS,RAW_SALESFORCE_USERS'
```

```console
--exclude-relations EXCLUDE_RELATIONS
# Filter relation(s) to exclude from source file(s) generation
```

```console
--destination DESTINATION
# Generated metadata destination path
```

```console
--no-prompt
# Silently generate metadata
```
