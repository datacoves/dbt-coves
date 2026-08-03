## dbt-coves data-sync redshift

### Overview

```console
dbt-coves data-sync redshift --source <dataset_name>
```

Syncs Airflow's metadata tables (see [data-sync overview](../) for the default table list and incremental/full-load strategy) into a Redshift database, using the `dlt` `sql_database` source and a Redshift `dlt` destination.

### Arguments

```console
--source
# Required. Name of the destination dataset (schema) in Redshift the tables are loaded into.
```

```console
--tables
# Optional, comma separated. Extra tables to sync in addition to the default Airflow table set.
```

### Required environment variables

Redshift destination credentials are read from environment variables (not CLI flags), following the `DATA_SYNC_REDSHIFT_*` naming convention:

```console
DATA_SYNC_REDSHIFT_DATABASE
DATA_SYNC_REDSHIFT_PASSWORD
DATA_SYNC_REDSHIFT_USER
DATA_SYNC_REDSHIFT_HOST
```

Unlike Snowflake, Redshift only supports password auth here - all four variables are required.

### Discussion

- These env vars are copied internally into `dlt`'s expected `DESTINATION__REDSHIFT__CREDENTIALS__*` variables (database, password, username, host) - you never need to set the `DESTINATION__*` ones yourself.
- `--source` doubles as the `dlt` pipeline's `dataset_name`, i.e. the schema that gets created/used in the destination database.

### Sample usage

```console
export DATA_SYNC_SOURCE_CONNECTION_STRING="postgresql://airflow:pass@airflow-db:5432/airflow"
export DATA_SYNC_REDSHIFT_DATABASE=analytics
export DATA_SYNC_REDSHIFT_HOST=my-cluster.abc123.us-east-1.redshift.amazonaws.com
export DATA_SYNC_REDSHIFT_USER=dlt_loader
export DATA_SYNC_REDSHIFT_PASSWORD=********

dbt-coves data-sync redshift --source airflow_metadata --tables xcom,trigger
```
