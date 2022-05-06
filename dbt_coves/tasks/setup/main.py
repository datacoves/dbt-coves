from rich.console import Console

from dbt_coves.tasks.setup.git import SetupGitTask

from .all import SetupAllTask
from .sqlfluff import SetupSqlfluffTask
from .pre_commit import SetupPrecommitTask
from .dbt import SetupDbtTask
from .ssh import SetupSSHTask
from .vs_code import SetupVscodeTask

from dbt_coves.tasks.base import NonDbtBaseTask

console = Console()


class SetupTask(NonDbtBaseTask):
    """
    Task that code-gen dbt resources
    """

    key_column_with = 20
    value_column_with = 50
    arg_parser = None

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        ext_subparser = sub_parsers.add_parser(
            "setup", parents=[base_subparser], help="Set up dbt-coves components."
        )
        ext_subparser.set_defaults(cls=cls, which="setup")
        sub_parsers = ext_subparser.add_subparsers(
            title="dbt-coves setup commands", dest="task"
        )
        SetupAllTask.register_parser(sub_parsers, base_subparser)
        SetupGitTask.register_parser(sub_parsers, base_subparser)
        SetupDbtTask.register_parser(sub_parsers, base_subparser)
        SetupSSHTask.register_parser(sub_parsers, base_subparser)
        SetupVscodeTask.register_parser(sub_parsers, base_subparser)
        SetupSqlfluffTask.register_parser(sub_parsers, base_subparser)
        SetupPrecommitTask.register_parser(sub_parsers, base_subparser)
        cls.arg_parser = ext_subparser
        return ext_subparser
