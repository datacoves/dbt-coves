from rich.console import Console

from dbt_coves.tasks.base import BaseConfiguredTask

from .airbyte import LoadAirbyteTask
from .fivetran import LoadFivetranTask

console = Console()


class LoadTask(BaseConfiguredTask):
    """
    Task that loads data from different systems
    """

    arg_parser = None

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        ext_subparser = sub_parsers.add_parser(
            "load",
            parents=[base_subparser],
            help="Loads configurations into different systems, such as Airbyte.",
        )
        ext_subparser.set_defaults(cls=cls, which="load")
        sub_parsers = ext_subparser.add_subparsers(title="dbt-coves load commands", dest="task")
        LoadAirbyteTask.register_parser(sub_parsers, base_subparser)
        LoadFivetranTask.register_parser(sub_parsers, base_subparser)
        cls.arg_parser = ext_subparser
        return ext_subparser
