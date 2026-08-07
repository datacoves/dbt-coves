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

### In Action

https://www.loom.com/share/3eb0d4b7a67341f6bd4f2e0c161a8e54?sid=c1db5cca-4977-4fdd-9e3f-63adb723e844

### Sample usage

```console
dbt-coves generate templates
# Then edit .dbt_coves/templates/*.yml / *.sql as needed, and reference the folder:
dbt-coves generate sources --templates-folder .dbt_coves/templates
```
