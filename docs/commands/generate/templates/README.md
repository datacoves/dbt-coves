## dbt-coves generate templates

### Overview

```console
dbt-coves generate templates
```

Copies dbt-coves' built-in Jinja generation templates into your project's `.dbt_coves/templates/` folder. These are the same templates `generate sources` and `generate properties` render by default; copying them locally gives you editable starting points to override the default behavior - for example, adding a `metadata:` key when generating property files, or changing indentation/formatting conventions.

This command takes no arguments. If a template file already exists at the destination, you're prompted per-file to skip, overwrite, overwrite all remaining files, or cancel.

### Files copied

```
model_props.yml           # used by `generate properties`
source_props.yml          # used by `generate sources` (the sources.yml file)
staging_model_props.yml   # used by `generate sources` (the staging model's .yml properties)
staging_model.sql         # used by `generate sources` (the staging model's .sql file)
```

Once copied, point `--templates-folder` (or the `templates_folder` setting) on `generate sources` / `generate properties` at `.dbt_coves/templates` to use your customized versions instead of the defaults.

### Source names in `generate sources` templates

`source_props.yml` and `staging_model.sql` get a `source` variable holding the names to use, so `sources.yml` and the staging model's `source()` call agree. dbt's `source()` lookup is case-sensitive, so they must match exactly.

| Variable | Use |
|---|---|
| `source.name`, `source.table` | The source and table names, written to `sources.yml` and passed to `source()` in the staging model. |
| `source.quote_schema`, `source.quote_identifier` | Whether `sources.yml` needs `quoting: schema: true` / `quoting: identifier: true`. |

On Snowflake, a schema or table whose name only uses uppercase letters, digits, `_` and `$` is written in lowercase without quoting (for example `RAW.ORDERS` becomes `raw` / `orders`). A name that was created quoted (lowercase, mixed case, spaces, ...) or is a [reserved word](https://docs.snowflake.com/en/sql-reference/reserved-keywords) that can't be used unquoted in a qualified name (such as `ORDER` or `TABLE`, but not `ACCOUNT`) keeps its exact case and is quoted, so dbt finds `RAW."my_table"` rather than `RAW.MY_TABLE`.

On Redshift and BigQuery, which quote identifiers by default, both use the exact schema and table names.

Templates copied before this variable existed use `relation.schema` / `relation.name` directly. On Snowflake their `source()` calls don't match the lowercase names in `sources.yml`, so update them to use `source.name` / `source.table`, or copy the templates again.

### In Action

https://www.loom.com/share/3eb0d4b7a67341f6bd4f2e0c161a8e54?sid=c1db5cca-4977-4fdd-9e3f-63adb723e844

### Sample usage

```console
dbt-coves generate templates
# Then edit .dbt_coves/templates/*.yml / *.sql as needed, and reference the folder:
dbt-coves generate sources --templates-folder .dbt_coves/templates
```
