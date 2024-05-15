import os

from dbt_coves.utils.tracking import trackable

from .base import BaseDataSyncTask

DLT_PREFIX = "DESTINATION__REDSHIFT__CREDENTIALS__"
DATA_SYNC_PREFIX = "DATA_SYNC_REDSHIFT_"


class RedshiftDestination(object):
    def set_credentials(self) -> None:
        """Dlt destination credentials can be set either by modifying the secrets file or
        by setting environment variables. Setting environment variables.
        """
        for key in ["DATABASE", "PASSWORD", "USER", "HOST"]:
            value = os.environ.get(f"{DATA_SYNC_PREFIX}{key}")
            assert value, f"Environment variable {DATA_SYNC_PREFIX}{key} is not defined"
            os.environ[f"{DLT_PREFIX}{key}"] = value
        value = os.environ.get(f"{DATA_SYNC_PREFIX}USER")
        assert value, f"Environment variable {DATA_SYNC_PREFIX}USER is not defined"
        os.environ[f"{DLT_PREFIX}USERNAME"] = value


class RedshiftDestinationDataSyncTask(BaseDataSyncTask):
    """
    Task that extracts airbyte sources, connections and destinations and stores them as json files
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "redshift",
            parents=[base_subparser],
            help="""Loads data into Redshift""",
        )
        subparser.add_argument("--source", help="Source database name", required=True)
        subparser.set_defaults(cls=cls, which="redshift")
        return subparser

    @trackable
    def run(self):
        self.get_source_connection_string()
        self.get_destination_instance()
        self.destination_instance.set_credentials()
        self.load_entire_database()
        return 0

    def get_destination_instance(self) -> None:
        self.destination = "redshift"
        self.destination_instance = RedshiftDestination()
