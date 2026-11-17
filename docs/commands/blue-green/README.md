## dbt-coves blue-green

### Overview

```console
dbt-coves blue-green
```

Performs a blue-green dbt deployment on **Snowflake**: it clones the production database's schemas and grants into a fresh staging database, runs `dbt build` against that staging database, then atomically swaps staging and production with Snowflake's `ALTER DATABASE ... SWAP WITH ...`. If anything in the build fails, production is never touched - the failed staging database is simply left in place (or dropped, see `--drop-staging-db-on-failure`) for inspection.

High-level flow:

1. Fail fast if a staging database with the target name already exists (unless `--drop-staging-db-at-start` is set).
2. Create the staging database and prepare it:
   - Copy production's database-level parameters (those explicitly set on the database, such as `DATA_RETENTION_TIME_IN_DAYS`) and comment. A parameter the deploy role isn't allowed to set (for example `LOG_LEVEL`, which needs the account-level `MODIFY LOG LEVEL` privilege) is skipped with a warning and reverts to its default after the swap.
   - Clone all of production's schemas.
   - Copy the schemas' grants, the schemas' and the database's future grants, and the database's inherited grants, except `OWNERSHIP`.

   Future grants are in place before the build so they apply to the objects dbt creates. Schema-level inherited grants are kept by the schema clone itself.
3. Run `dbt build` with the target database pointed at staging. The deploy role still owns the staging schemas during the build.
4. Finish the staging database:
   - Copy the (possibly changed) database-level grants.
   - Apply future `OWNERSHIP` grants.
   - Re-enable `MANAGED ACCESS` on schemas that have it in production. A schema clone doesn't keep it, and it's held back until after the build so dbt can still grant on the objects it creates.
   - Transfer schema ownership, then database ownership, with `COPY CURRENT GRANTS`.

   Only grants to account roles are copied. If production's database or schemas are granted to a database role, share, user or application, the deploy stops before `dbt build` with an error listing those grants, because swapping would silently drop them. Future and inherited grants to anything other than an account role are skipped with a warning.
5. Swap staging and production in Snowflake.
6. Drop the now-old production database (physically the object still named "staging") unless `--keep-staging-db-on-success` is set.

If step 3-5 raises, the exception propagates (and, depending on `--drop-staging-db-on-failure`, the staging database is dropped first) - production is left untouched because the swap never happened.

### Arguments

`dbt-coves blue-green` supports the following args:

```console
--prod-db-env-var
# Name of the environment variable that holds the production database name, e.g. 'SNOWFLAKE_DATABASE'.
# dbt-coves reads os.environ[<that var>] to learn which database is "blue"; it's also
# temporarily overwritten with the staging database name while `dbt build` runs.
```

```console
--staging-database
# Explicit name for the staging ("green") database.
# Mutually exclusive with --staging-suffix.
```

```console
--staging-suffix
# Suffix appended to the production database name to derive the staging database name,
# e.g. suffix 'STAGING' on database 'ANALYTICS' -> 'ANALYTICS_STAGING'.
# Mutually exclusive with --staging-database. If neither is given, defaults to suffix 'STAGING'.
```

```console
--drop-staging-db-at-start
# Flag: if a staging database with the target name already exists, drop it and continue
# instead of aborting with an error.
```

```console
--drop-staging-db-on-failure
# Flag: if the dbt build (or any step before the swap) fails, drop the staging database
# before re-raising the error, instead of leaving it around for debugging.
```

```console
--keep-staging-db-on-success
# Flag: after a successful swap, keep the ex-production database (left under the staging name)
# instead of dropping it. Useful for keeping a rollback copy of the previous production data.
```

```console
--dbt-selector
# dbt selector(s) passed to `dbt build`, e.g. '-s my_project'.
# Ignored when running in deferral mode (see Discussion below).
```

```console
--full-refresh
# Flag: pass --full-refresh to the `dbt build` run.
```

```console
--defer
# Flag: run `dbt build` in deferral mode against previous state instead of using --dbt-selector.
```

### Discussion

- This command is Snowflake-only - it opens its own `snowflake.connector` connection (reusing the credentials from your dbt profile/adapter) to run the `SHOW DATABASES`, `CREATE DATABASE`, grant-cloning, and `ALTER DATABASE ... SWAP WITH ...` statements outside of dbt itself.
- Deferral is triggered either by passing `--defer`, or automatically when the `MANIFEST_FOUND` environment variable is set to `"true"` (as Datacoves' CI does for Slim CI runs). In deferral mode, dbt-coves runs `dbt build --defer --state logs -s state:modified+ --fail-fast` and `--dbt-selector` is ignored; otherwise it runs `dbt build --fail-fast <your --dbt-selector, split on spaces>`.
- All arguments can also be set under `blue_green:` in `.dbt_coves/config.yml`, which is the more common approach for this command since it's normally invoked unattended from CI/Airflow rather than typed by hand.
- `--staging-database` and `--staging-suffix` are mutually exclusive; so are the concepts they represent - dbt-coves raises immediately if both resolve to a value, or if the computed staging name collides with the production name.
- Views must reference other models without the database name, or they break after the swap: see [Views and the swap](#views-and-the-swap).

### Views and the swap

dbt writes fully qualified names into a view's SQL. A view built during the deploy therefore selects from `ANALYTICS_STAGING.<schema>.<model>`. After the swap that name belongs to the old production database, which is then dropped (unless `--keep-staging-db-on-success`), and the view fails with `Database 'ANALYTICS_STAGING' does not exist or not authorized`. Tables aren't affected, since they hold data rather than a query.

To avoid this, override `ref()` in your project so models in the staging database are referenced without the database name. Snowflake resolves those relative to the view's own database, so they keep working after the swap:

```sql
-- macros/ref.sql
{% macro ref() %}
    {% set version = kwargs.get('version') or kwargs.get('v') %}
    {% set packagename = none %}
    {%- if (varargs | length) == 1 -%}
        {% set modelname = varargs[0] %}
    {%- else -%}
        {% set packagename = varargs[0] %}
        {% set modelname = varargs[1] %}
    {% endif %}

    {% if packagename is not none %}
        {% set rel = builtins.ref(packagename, modelname, version=version) %}
    {% else %}
        {% set rel = builtins.ref(modelname, version=version) %}
    {% endif %}

    {# Match your staging database name (here: the default STAGING suffix). #}
    {% set db_name = rel.database | lower %}
    {% if db_name.startswith('staging') or db_name.endswith('staging') %}
        {% do return(rel.include(database=false)) %}
    {% else %}
        {% do return(rel) %}
    {% endif %}
{% endmacro %}
```

References to other databases, such as `source()`s in a raw database, keep the database name and aren't affected. A `source()` that points into the database being deployed has the same problem as `ref()`.

### Sample usage

```console
# .dbt_coves/config.yml
blue_green:
  prod_db_env_var: SNOWFLAKE_DATABASE
  staging_suffix: STAGING
  drop_staging_db_at_start: true
  drop_staging_db_on_failure: true
  dbt_selector: "-s +my_project"
```

```console
dbt-coves blue-green
```

```console
# Equivalent, entirely via flags
dbt-coves blue-green \
  --prod-db-env-var SNOWFLAKE_DATABASE \
  --staging-suffix STAGING \
  --drop-staging-db-at-start \
  --drop-staging-db-on-failure \
  --dbt-selector "-s +my_project"
```
