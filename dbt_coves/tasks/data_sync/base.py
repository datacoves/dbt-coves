"""
Bring all common (shared across Snowflake and Redshift) dlt functionalities here
"""

import os
from datetime import datetime

import dlt
from dlt.sources.credentials import ConnectionStringCredentials
from rich.console import Console

from dbt_coves.tasks.base import NonDbtBaseConfiguredTask

from .sql_database import sql_database, sql_table

# These tables are always synced.  Anything else can be requested but that's on user.
DEFAULT_AIRFLOW_TABLES = [
    "ab_permission",
    "ab_role",
    "ab_user",
    "dag",
    "dag_run",
    "dag_tag",
    "import_error",
    "job",
    "task_fail",
    "task_instance",
]

# These tables have a column appropriate for incremental upload.  Everything else will just
# blindly replace the destination table.  Each tuple is (column, initial_value).
AIRFLOW_INCREMENTALS = {
    "dag": ("last_pickled", "1970-01-01T00:00:00Z"),
    "dag_run": ("execution_date", datetime.fromisoformat("1970-01-01T00:00:00Z")),
    "import_error": ("timestamp", datetime.fromisoformat("1970-01-01T00:00:00Z")),
    "job": ("start_date", datetime.fromisoformat("1970-01-01T00:00:00Z")),
    "task_fail": ("start_date", datetime.fromisoformat("1970-01-01T00:00:00Z")),
    "task_instance": ("updated_at", datetime.fromisoformat("1970-01-01T00:00:00Z")),
}

console = Console()


class BaseDataSyncTask(NonDbtBaseConfiguredTask):
    def get_source_connection_string(self):
        self.source_connection_string = os.environ.get("DATA_SYNC_SOURCE_CONNECTION_STRING")
        assert self.source_connection_string, (
            "Environment variable " "DATA_SYNC_SOURCE_CONNECTION_STRING is not defined"
        )

    def perform_sync(self) -> None:
        """Use the sql_database source to completely load all tables in a database"""
        # Merge the default table list with the user-requested table list, and split it into
        # incremental and full loads according to if we have an incremental column.
        full_tables = []
        incremental_tables = {}
        requested_tables = self.tables
        for i in DEFAULT_AIRFLOW_TABLES + requested_tables:
            if i in AIRFLOW_INCREMENTALS.keys():
                incremental_tables[i] = AIRFLOW_INCREMENTALS[i]
            else:
                if i not in full_tables:
                    full_tables.append(i)

        pipeline = dlt.pipeline(
            progress="enlighten",
            pipeline_name="source",
            destination=self.destination,
            dataset_name=self.args.source,
        )

        # By default the sql_database source reflects all tables in the schema
        # The database credentials are sourced from the `.dlt/secrets.toml` configuration
        credentials = ConnectionStringCredentials(self.source_connection_string)

        # All fully-replaced tables go at once.
        if len(full_tables):
            console.print(f"Loading full tables into {self.destination}")
            console.print("Full tables: ", str(full_tables))
            source = sql_database(credentials=credentials, table_names=full_tables)
            console.print("Run pipeline")
            info = pipeline.run(source, write_disposition="replace")
            print(info)
        # Incrementally loaded tables go one at a time, with their own cursor columns.
        for i in incremental_tables:
            console.print(f"Incrementally loading table: {i}")
            source = sql_table(
                credentials=credentials,
                table=i,
                incremental=dlt.sources.incremental(
                    incremental_tables[i][0], initial_value=incremental_tables[i][1]
                ),
            )
            info = pipeline.run(source, write_disposition="append")
            print(info)
