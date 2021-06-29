from cookiecutter.main import cookiecutter
from rich.console import Console

from .base import BaseTask

console = Console()


class InitTask(BaseTask):
    """
    Task that clones and applies a cookiecutter template
    """
    
    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "init",
            parents=[base_subparser],
            help="Initializes a new dbt project using predefined conventions.",
        )
        subparser.add_argument(
            "--template",
            type=str,
            help="Cookiecutter template github url, i.e."
                 " 'https://github.com/datacoves/cookiecutter-dbt-coves.git'",
        )
        subparser.set_defaults(cls=cls, which="init")
        return subparser

    def run(self) -> int:
        template_url = self.coves_flags.init["template"]
        console.print(f"Applying cookiecutter template {template_url} to your project...\n")
        cookiecutter(template_url)
        return 0
