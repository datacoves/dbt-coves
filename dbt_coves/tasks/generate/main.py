from rich.console import Console

from dbt_coves.tasks.base import BaseConfiguredTask

from .metadata import GenerateMetadataTask
from .properties import GeneratePropertiesTask
from .sources import GenerateSourcesTask
from .templates import GenerateTemplatesTask

console = Console()


class GenerateTask(BaseConfiguredTask):
    """
    Task that code-gen dbt resources
    """

    arg_parser = None

    # "Generate" has now multiple sub tasks.
    tasks = [
        GeneratePropertiesTask,
        GenerateSourcesTask,
        GenerateMetadataTask,
        GenerateTemplatesTask,
    ]

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        gen_subparser = sub_parsers.add_parser(
            "generate",
            parents=[base_subparser],
            help="""Generates dbt source and staging files, model property files and extracts
             metadata for tables (used as input for sources and properties)""",
        )
        gen_subparser.add_argument(
            "--no-prompt",
            help="Silently generate dbt-coves resources",
            action="store_true",
            default=False,
        )
        gen_subparser.set_defaults(cls=cls, which="generate")
        sub_parsers = gen_subparser.add_subparsers(title="dbt-coves generate commands", dest="task")

        # Register a separate sub parser for each sub task.
        [x.register_parser(sub_parsers, base_subparser) for x in cls.tasks]
        cls.arg_parser = gen_subparser
        return gen_subparser
