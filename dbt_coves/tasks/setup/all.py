import os
from pathlib import Path

from rich.console import Console
from dbt_coves.tasks.base import NonDbtBaseTask

from .ssh import SetupSSHTask
from .git import SetupGitTask
from .dbt import SetupDbtTask

console = Console()


class SetupAllTask(NonDbtBaseTask):
    """
    Task that runs ssh key generation, git repo clone and db connection setup
    """

    key_column_with = 50
    value_column_with = 30

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "all",
            parents=[base_subparser],
            help="Set up a complete dbt-coves project.",
        )
        subparser.add_argument(
            "--templates",
            type=str,
            help="Location of your sqlfluff, ci and pre-commit config files",
        )
        subparser.add_argument(
            "--open-ssl-public-key",
            help="Generate and output OpenSSL key alongside Git OpenSSH one",
            action="store_true",
            default=False,
        )
        subparser.set_defaults(cls=cls, which="all")
        return subparser

    def run(self) -> int:
        """
        Env vars that can be set: USER_FULLNAME, USER_EMAIL, WORKSPACE_PATH, GIT_REPO_URL, DBT_PROFILES_DIR
        """
        workspace_path = os.environ.get("WORKSPACE_PATH", Path.cwd())

        SetupSSHTask(self.args, self.coves_config).run()

        SetupGitTask(self.args, self.coves_config).run(workspace_path)

        SetupDbtTask.dbt_init()

        SetupDbtTask.dbt_debug()

        SetupDbtTask.dbt_deps()

        return 0

    def get_config_value(self, key):
        return self.coves_config.integrated["setup"]["all"][key]
