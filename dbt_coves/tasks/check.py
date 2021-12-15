import questionary
from rich.console import Console

from dbt_coves.tasks.fix import fix
from dbt_coves.utils.shell import run as shell_run

from .base import BaseConfiguredTask

console = Console()


class CheckTask(BaseConfiguredTask):
    """
    Task that runs pre-commit and sqlfluff
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "check", parents=[base_subparser], help="Runs pre-commit hooks and linters."
        )
        subparser.add_argument(
            "--no-fix",
            help="Do not suggest auto-fixing linting errors. Useful when running this command on CI jobs.",
            action="store_true",
            default=False,
        )
        subparser.set_defaults(cls=cls, which="check")
        return subparser

    def run(self) -> int:
        console.print(
            "Running pre-commit hooks on staged and commmitted git files...\n"
        )

        command = shell_run(["pre-commit", "run", "-a"])
        if command.returncode != 0:
            return command.returncode

        sql_fluff_status = 0
        for source_path in self.config.source_paths:
            console.print(f"Linting files in [u]{source_path}[/u]...\n")

            command = shell_run(["sqlfluff", "lint", source_path])

            if command.returncode != 0:
                sql_fluff_status = command.returncode
            if not self.coves_flags.check["no-fix"] and command.returncode != 0:
                command = fix(source_path)
                if command.returncode != 0:
                    return command.returncode
        return sql_fluff_status
