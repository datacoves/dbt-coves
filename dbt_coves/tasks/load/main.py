from shutil import copytree

from rich.console import Console

from dbt_coves.tasks.base import BaseConfiguredTask

from .airbyte import LoadAirbyteTask

console = Console()


class LoadTask(BaseConfiguredTask):
    """
    Task that loads data from different systems
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        ext_subparser = sub_parsers.add_parser(
            "load",
            parents=[base_subparser],
            help="Loads data from different systems.",
        )
        ext_subparser.set_defaults(cls=cls, which="load")
        sub_parsers = ext_subparser.add_subparsers(
            title="dbt-coves load commands", dest="task"
        )
        LoadAirbyteTask.register_parser(sub_parsers, base_subparser)
        return ext_subparser
