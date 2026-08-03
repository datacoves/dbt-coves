## dbt-coves data-sync snowflake

### Overview

```console
dbt-coves data-sync snowflake --source <dataset_name>
```

Syncs Airflow's metadata tables (see [data-sync overview](../) for the default table list and incremental/full-load strategy) into a Snowflake database, using the `dlt` `sql_database` source and a Snowflake `dlt` destination.

### Arguments

```console
--source
# Required. Name of the destination dataset (schema) in Snowflake the tables are loaded into.
```

```console
--tables
# Optional, comma separated. Extra tables to sync in addition to the default Airflow table set.
```

### Required environment variables

Snowflake destination credentials are read from environment variables (not CLI flags), following the `DATA_SYNC_SNOWFLAKE_*` naming convention:

```console
DATA_SYNC_SNOWFLAKE_DATABASE
DATA_SYNC_SNOWFLAKE_WAREHOUSE
DATA_SYNC_SNOWFLAKE_ROLE
DATA_SYNC_SNOWFLAKE_USER
DATA_SYNC_SNOWFLAKE_ACCOUNT
```

Plus one of:

```console
DATA_SYNC_SNOWFLAKE_PRIVATE_KEY          # and optionally DATA_SYNC_SNOWFLAKE_PRIVATE_KEY_PASSWORD
# or
DATA_SYNC_SNOWFLAKE_PASSWORD
```

If neither `DATA_SYNC_SNOWFLAKE_PRIVATE_KEY` nor `DATA_SYNC_SNOWFLAKE_PASSWORD` is set, the command fails before attempting to load. Key-pair auth takes priority when both are present.

### Discussion

- These env vars are copied internally into `dlt`'s expected `DESTINATION__SNOWFLAKE__CREDENTIALS__*` variables (database, warehouse, role, username, host, and password/private key) - you never need to set the `DESTINATION__*` ones yourself, only the `DATA_SYNC_SNOWFLAKE_*` ones above.
- `--source` doubles as the `dlt` pipeline's `dataset_name`, i.e. the schema that gets created/used in the destination database.

### Sample usage

```console
export DATA_SYNC_SOURCE_CONNECTION_STRING="postgresql://airflow:pass@airflow-db:5432/airflow"
export DATA_SYNC_SNOWFLAKE_DATABASE=ANALYTICS
export DATA_SYNC_SNOWFLAKE_WAREHOUSE=LOADING
export DATA_SYNC_SNOWFLAKE_ROLE=LOADER
export DATA_SYNC_SNOWFLAKE_USER=DLT_LOADER
export DATA_SYNC_SNOWFLAKE_ACCOUNT=my_account
export DATA_SYNC_SNOWFLAKE_PASSWORD=********

dbt-coves data-sync snowflake --source airflow_metadata --tables xcom,trigger
```
