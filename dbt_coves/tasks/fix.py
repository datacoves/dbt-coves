import sys

from rich.console import Console

from dbt_coves.utils.shell import run as shell_run

from .base import BaseConfiguredTask

console = Console()


def fix(source_path):
    return shell_run(["sqlfluff", "fix", "-f", source_path])


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
        for source_path in self.config.source_paths:
            console.print(f"Trying to auto-fix linting errors in [u]{source_path}[/u]...\n")
            fix(source_path)
        return 0
