import os

from dbt_coves.utils.tracking import trackable

from .base import BaseDataSyncTask

DLT_PREFIX = "DESTINATION__DUCKDB__CREDENTIALS__"
DATA_SYNC_PREFIX = "DATA_SYNC_DUCKDB_"


class DuckdbDestination(object):
    def set_credentials(self) -> None:
        # No credentials required for duckdb
        pass

class DuckdbDataSyncTask(BaseDataSyncTask):
    """
    Task that extracts DuckDB sources, connections and destinations and stores them as json files
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "duckdb",
            parents=[base_subparser],
            help="""Loads data into DuckDB""",
        )
        subparser.add_argument("--tables", help="List of tables to dump", required=False)
        subparser.add_argument("--source", help="Source database name", required=True)
        subparser.set_defaults(cls=cls, which="duckdb")
        return subparser

    def get_config_value(self, key):
        return self.coves_config.integrated["data_sync"][self.args.task][key]

    @trackable
    def run(self):
        self.tables = self.get_config_value("tables")
        self.get_source_connection_string()
        self.get_destination_instance()
        self.destination_instance.set_credentials()
        self.perform_sync()
        return 0

    def get_destination_instance(self) -> None:
        self.destination = "duckdb"
        self.destination_instance = DuckdbDestination()
