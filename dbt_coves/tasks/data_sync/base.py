"""
Bring all common (shared across Snowflake and Redshift) dlt functionalities here
"""

import os

import dlt
from dlt.sources.credentials import ConnectionStringCredentials
from rich.console import Console

from dbt_coves.tasks.base import NonDbtBaseConfiguredTask

from .sql_database import sql_database

console = Console()


class BaseDataSyncTask(NonDbtBaseConfiguredTask):
    def get_source_connection_string(self):
        self.source_connection_string = os.environ.get("DATA_SYNC_SOURCE_CONNECTION_STRING")
        assert self.source_connection_string, (
            "Environment variable " "DATA_SYNC_SOURCE_CONNECTION_STRING is not defined"
        )

    def perform_sync(self) -> None:
        """Use the sql_database source to completely load all tables in a database"""
        pipeline = dlt.pipeline(
            progress="enlighten",
            pipeline_name="source",
            destination=self.destination,
            dataset_name=self.args.source,
        )

        # By default the sql_database source reflects all tables in the schema
        # The database credentials are sourced from the `.dlt/secrets.toml` configuration
        credentials = ConnectionStringCredentials(self.source_connection_string)
        source = sql_database(credentials=credentials, table_names=self.tables or None)

        # Run the pipeline. For a large db this may take a while
        console.print(f"Dumping database to {self.destination}")
        info = pipeline.run(source, write_disposition="replace")
        print(info)
