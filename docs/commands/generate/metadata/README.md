## dbt-coves generate metadata

This command will generate a `dbt-coves metadata` CSV file for your database table(s). This can then be used to collect descriptions from stakeholders and later used as an input to other dbt-coves commands such as `dbt-coves generate sources`

The`Metadata` file consists of comma-separated values in which the user can specify column(s) keys and descriptions and is particularly useful for working with stakeholders to get descriptions for dbt YML files when [generate sources](../sources/README.md#metadata) or [generate properties](../properties/README.md#metadata) is used.

### Arguments

`dbt-coves generate metadata` supports the following args:

```console
--database DATABASE
# Database where source relations live, if different than the dbt target
```

```console
--schemas SCHEMAS
# Comma separated list of schemas where raw data resides, i.e. 'RAW_SALESFORCE,RAW_HUBSPOT'
```

```console
--select-relations SELECT_RELATIONS
# Comma separated list of relations where raw data resides, i.e. 'hubspot_products,salesforce_users'
```

```console
--exclude-relations EXCLUDE_RELATIONS
# Filter relation(s) to exclude from source file(s) generation
```

```console
--destination DESTINATION
# Generated metadata file destination path
```

```console
--no-prompt
# Silently generate metadata
```
