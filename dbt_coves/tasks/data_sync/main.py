from rich.console import Console

from dbt_coves.tasks.base import BaseTask

from .redshift import RedshiftDataSyncTask
from .snowflake import SnowflakeDataSyncTask

console = Console()


class DataSyncTask(BaseTask):
    """
    Task that extracts data from source and loads into destination
    """

    arg_parser = None

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "data-sync",
            parents=[base_subparser],
            help="Extract data from a source and load into a destination",
        )
        subparser.set_defaults(cls=cls, which="data_sync")
        sub_parsers = subparser.add_subparsers(title="dbt-coves data-sync commands", dest="task")
        SnowflakeDataSyncTask.register_parser(sub_parsers, base_subparser)
        RedshiftDataSyncTask.register_parser(sub_parsers, base_subparser)
        cls.arg_parser = subparser
        return subparser
