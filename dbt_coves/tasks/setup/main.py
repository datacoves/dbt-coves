from rich.console import Console

from dbt_coves.tasks.base import NonDbtBaseTask

from .dbt import SetupDbtTask
from .git import SetupGitTask
from .pre_commit import SetupPrecommitTask
from .ssh import SetupSSHTask

console = Console()


class DbtCovesSetupException(Exception):
    pass


class SetupTask(NonDbtBaseTask):
    """
    Task that code-gen dbt resources
    """

    tasks = [
        SetupGitTask,
        SetupDbtTask,
        SetupSSHTask,
        SetupPrecommitTask,
    ]

    key_column_with = 20
    value_column_with = 50
    arg_parser = None

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        ext_subparser = sub_parsers.add_parser(
            "setup",
            parents=[base_subparser],
            help="Set up project components (git, dbt, vscode, sqlfluff, pre-commit, etc)",
        )
        ext_subparser.set_defaults(cls=cls, which="setup")
        sub_parsers = ext_subparser.add_subparsers(title="dbt-coves setup commands", dest="task")
        # Register a separate sub parser for each sub task.
        [x.register_parser(sub_parsers, base_subparser) for x in cls.tasks]
        cls.arg_parser = ext_subparser
        return ext_subparser
