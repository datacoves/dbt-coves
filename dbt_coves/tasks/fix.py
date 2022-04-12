import sys

from rich.console import Console

from dbt_coves.core.exceptions import DbtCovesException
from dbt_coves.utils.shell import run as shell_run

from .base import BaseConfiguredTask

console = Console()


def fix(model_path):
    return shell_run(["sqlfluff", "fix", "-f", model_path])


class FixTask(BaseConfiguredTask):
    """
    Task that runs sqlfluff fix
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "fix", parents=[base_subparser], help="Runs linter fixes."
        )
        subparser.set_defaults(cls=cls, which="fix")
        return subparser

    def run(self) -> int:
        for model_path in self.config.model_paths:
            console.print(
                f"Trying to auto-fix linting errors in [u]{model_path}[/u]...\n"
            )
            fix(model_path)
        return 0
