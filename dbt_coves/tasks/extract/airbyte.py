from rich.console import Console

from dbt_coves.tasks.base import BaseConfiguredTask

console = Console()


class ExtractAirbyteTask(BaseConfiguredTask):
    """
    Task that extracts airbyte sources, connections and destinations and stores them as json files
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "airbyte",
            parents=[base_subparser],
            help="Extracts airbyte sources, connections and destinations and stores them as json files",
        )
        subparser.add_argument(
            "--destination",
            type=str,
            help="Where json files will be generated, i.e. " "'airbyte'",
        )
        subparser.set_defaults(cls=cls, which="airbyte")
        return subparser

    def run(self):
        config_destination = self.get_config_value("destination")

    def get_config_value(self, key):
        return self.coves_config.integrated["extract"]["airbyte"][key]
