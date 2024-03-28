import os

import dlt
from dlt.sources.credentials import ConnectionStringCredentials

from dbt_coves.tasks.base import BaseTask
from dbt_coves.utils.tracking import trackable

from .sql_database import sql_database


class SnowflakeDestination(object):
    def set_credentials(self) -> None:
        """Dlt destination credentials can be set either by modifying the secrets file or
        by setting environment variables. Setting environment variables.
        """
        dlt_prefix = "DESTINATION__SNOWFLAKE__CREDENTIALS__"
        data_sync_prefix = "DATA_SYNC_SNOWFLAKE_"
        for key in ["DATABASE", "PASSWORD", "WAREHOUSE", "ROLE"]:
            value = os.environ.get(f"{data_sync_prefix}{key}")
            assert value, f"Environment variable {data_sync_prefix}{key} is not defined"
            os.environ[f"{dlt_prefix}{key}"] = value
        value = os.environ.get(f"{data_sync_prefix}USER")
        assert value, f"Environment variable {data_sync_prefix}USER is not defined"
        os.environ[f"{dlt_prefix}USERNAME"] = value
        value = os.environ.get(f"{data_sync_prefix}ACCOUNT")
        assert value, f"Environment variable {data_sync_prefix}ACCOUNT is not defined"
        os.environ[f"{dlt_prefix}HOST"] = value


class SnowflakeDestinationDataSyncTask(BaseTask):
    """
    Task that extracts airbyte sources, connections and destinations and stores them as json files
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "snowflake",
            parents=[base_subparser],
            help="""Loads data into Snowflake""",
        )
        subparser.add_argument(
            "--destination", help="Destination type: i.e. 'snowflake'", required=False
        )
        subparser.add_argument(
            "--destination-database", help="Destination database name", required=True
        )
        subparser.set_defaults(cls=cls, which="snowflake")
        return subparser

    @trackable
    def run(self):
        self.get_source_connection_string()
        self.get_destination_instance()
        self.destination_instance.set_credentials()
        self.load_entire_database()
        return 0

    def get_source_connection_string(self):
        self.source_connection_string = os.environ.get("DATA_SYNC_SOURCE_CONNECTION_STRING")
        assert self.source_connection_string, (
            "Environment variable " "DATA_SYNC_SOURCE_CONNECTION_STRING is not defined"
        )

    def get_destination_instance(self) -> None:
        self.destination_instance = SnowflakeDestination()

    def load_entire_database(self) -> None:
        """Use the sql_database source to completely load all tables in a database"""
        pipeline = dlt.pipeline(
            pipeline_name="source",
            destination="snowflake",
            dataset_name=self.args.destination_database,
        )

        # By default the sql_database source reflects all tables in the schema
        # The database credentials are sourced from the `.dlt/secrets.toml` configuration
        credentials = ConnectionStringCredentials(self.source_connection_string)
        source = sql_database(credentials=credentials)

        # Run the pipeline. For a large db this may take a while
        info = pipeline.run(source, write_disposition="replace")
        print(info)
