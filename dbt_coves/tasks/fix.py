import sys

from rich.console import Console

from dbt_coves.core.exceptions import DbtCovesException
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
        objs_to_fix = self.config.get("source_paths", self.config.get("model_paths"))
        if not objs_to_fix:
            raise DbtCovesException(
                "Could not find [u]source_paths[/u] or [u]model_paths[/u] in [u]dbt_project.yml[/u] file"
            )
        for obj in objs_to_fix:
            console.print(f"Trying to auto-fix linting errors in [u]{obj}[/u]...\n")
            fix(obj)
        return 0
