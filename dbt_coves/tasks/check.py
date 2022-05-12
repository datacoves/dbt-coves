import questionary
from rich.console import Console

from dbt_coves.tasks.fix import fix
from dbt_coves.utils.shell import run as shell_run, run_and_capture

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
        subparser.add_argument(
            "--slim",
            help="Run precommit checks only on changed model files. This requires a manifest for comparison \
                via dbt state:modified. We expect this in the dbt project directory in a subdirectory called logs.",
            action="store_true",
            default=False,
        )
        subparser.set_defaults(cls=cls, which="check")
        return subparser

    def run(self) -> int:
        console.print(
            "Running pre-commit hooks on staged and commmitted git files...\n"
        )

        if self.coves_flags.check["slim"]:
            dbt_ls = [
                "dbt",
                "ls",
                "--select",
                "state:modified",
                "--state",
                "logs",
                "--project-dir",
                self.config.project_root,
                "--resource-type",
                "model",
                "--output",
                "path",
            ]
            command = shell_run(
                [
                    "pre-commit",
                    "run",
                    "--files",
                    *[
                        change
                        for change in map(
                            lambda f: f"{self.config.project_root}/{f}",
                            run_and_capture(dbt_ls).stdout.splitlines(),
                        )
                    ],
                ]
            )
        else:
            command = shell_run(["pre-commit", "run", "--all-files"])
        if command.returncode != 0:
            return command.returncode

        sql_fluff_status = 0
        for model_path in self.config.model_paths:
            console.print(f"Linting files in [u]{model_path}[/u]...\n")

            command = shell_run(["sqlfluff", "lint", model_path])

            if command.returncode != 0:
                sql_fluff_status = command.returncode
            if not self.coves_flags.check["no-fix"] and sql_fluff_status != 0:
                confirmed = questionary.confirm(
                    f"Would you like to try auto-fixing linting errors in {model_path}?",
                    default=True,
                ).ask()
                if confirmed:
                    command = fix(model_path)
                    if command.returncode != 0:
                        return command.returncode
        return sql_fluff_status
