from pathlib import Path

import copier
import questionary
from rich.console import Console

from dbt_coves.utils.tracking import trackable

from .main import NonDbtBaseTask
from .utils import get_dbt_projects, get_git_root

console = Console()


class SetupPrecommitException(Exception):
    pass


class SetupPrecommitTask(NonDbtBaseTask):
    """
    Task that runs pre-commit setup
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "precommit",
            parents=[base_subparser],
            help="Set up pre-commit for dbt-coves project",
        )
        subparser.set_defaults(cls=cls, which="precommit")
        return subparser

    @trackable
    def run(self) -> int:
        self.setup_precommit()
        return 0

    @classmethod
    def setup_precommit(self):
        repo_root = get_git_root()
        dbt_project_paths = get_dbt_projects(repo_root)
        if not dbt_project_paths:
            raise SetupPrecommitException(
                "Your repository doesn't contain any dbt project where to install pre-commit into"
            )
        data = {}
        if len(dbt_project_paths) == 1:
            data["dbt_project_dir"] = dbt_project_paths
        else:
            data["dbt_project_dir"] = questionary.select(
                "In which dbt project would you like to install pre-commit?",
                choices=dbt_project_paths,
            ).ask()
        copier.run_auto(
            src_path=str(Path(__file__).parent.joinpath("templates", "pre_commit").resolve()),
            dst_path=repo_root,
            data=data,
        )
