from shutil import copytree

from rich.console import Console

from dbt_coves.tasks.base import BaseConfiguredTask

from .sources import GenerateSourcesTask

console = Console()


class GenerateTask(BaseConfiguredTask):
    """
    Task that code-gen dbt resources
    """
    
    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        gen_subparser = sub_parsers.add_parser(
            "generate", parents=[base_subparser], help="Generates sources and models with defaults."
        )
        gen_subparser.set_defaults(cls=cls, which="generate")
        sub_parsers = gen_subparser.add_subparsers(title="dbt-coves generate commands", dest="task")
        GenerateSourcesTask.register_parser(sub_parsers, base_subparser)
        return gen_subparser
