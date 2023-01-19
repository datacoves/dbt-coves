from rich.console import Console

from dbt_coves.tasks.base import BaseConfiguredTask

from .airbyte import ExtractAirbyteTask
from .fivetran import ExtractFivetranTask

console = Console()


class ExtractTask(BaseConfiguredTask):
    """
    Task that extracts data from different systems
    """

    arg_parser = None

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        ext_subparser = sub_parsers.add_parser(
            "extract",
            parents=[base_subparser],
            help="Extract configuration data from Airbyte or Fivetran",
        )
        ext_subparser.set_defaults(cls=cls, which="extract")
        sub_parsers = ext_subparser.add_subparsers(title="dbt-coves extract commands", dest="task")
        ExtractAirbyteTask.register_parser(sub_parsers, base_subparser)
        ExtractFivetranTask.register_parser(sub_parsers, base_subparser)
        cls.arg_parser = ext_subparser
        return ext_subparser
