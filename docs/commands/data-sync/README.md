# Data sync

## Overview

`dbt-coves data-sync` uploads Airflow's own metadata tables (DAGs, DAG runs, task instances, etc.) from Airflow's backend database into your analytics warehouse, so you can build dbt models on top of your own orchestration history - run durations, failure rates, DAG activity, and so on.

```console
dbt-coves data-sync <destination>
```

Where `destination` is one of:

- [_snowflake_](snowflake/): sync Airflow tables into Snowflake
- [_redshift_](redshift/): sync Airflow tables into Redshift

Under the hood this uses [`dlt`](https://dlthub.com/)'s `sql_database` source against Airflow's metadata database, and loads into the chosen destination via a `dlt` pipeline.

## Default tables

Regardless of destination, the following Airflow metadata tables are always synced:

```
ab_permission, ab_role, ab_user, dag, dag_run, dag_tag,
import_error, job, task_fail, task_instance
```

Use `--tables` to sync additional tables on top of this default set (see each destination's page for the flag).

## Load strategy

Tables are split into two groups:

- **Incrementally loaded** - `dag`, `dag_run`, `import_error`, `job`, `task_fail`, `task_instance` each have a known cursor column (e.g. `task_instance.updated_at`, `dag_run.execution_date`) and are loaded with `dlt`'s incremental cursor, appending only new/changed rows since the last run.
- **Fully replaced** - every other table (the rest of the default set, plus anything extra you pass via `--tables`) is reloaded in full each run (`write_disposition="replace"`).

## Source connection

Regardless of destination, the source (Airflow's metadata DB) connection string is always read from the environment variable:

```console
DATA_SYNC_SOURCE_CONNECTION_STRING
# SQLAlchemy-style connection string to Airflow's metadata database,
# e.g. postgresql://user:pass@host:5432/airflow
```

Destination credentials differ per target - see [snowflake](snowflake/) or [redshift](redshift/).
