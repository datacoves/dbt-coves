import os
from pathlib import Path

import copier
from rich.console import Console

from dbt_coves.config.config import DbtCovesConfig
from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.shell import run_and_capture_cwd  # noqa: F401 # might use it later

from .utils import file_exists

console = Console()


class SetupSqlfluffTask(NonDbtBaseTask):
    """
    Task that runs sqlfluff setup
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "sqlfluff",
            parents=[base_subparser],
            help="Set up sqlfluff for dbt-coves project",
        )
        subparser.set_defaults(cls=cls, which="sqlfluff")
        return subparser

    @classmethod
    def run(cls) -> int:
        config_folder = cls.get_config_folder(mandatory=False)
        cls.setup_sqlfluff(config_folder)
        return 0

    @classmethod
    def get_config_folder(cls, mandatory=True):
        workspace_path = os.environ.get("WORKSPACE_PATH", Path.cwd())
        return DbtCovesConfig.get_config_folder(workspace_path=workspace_path, mandatory=mandatory)

    @classmethod
    def setup_sqlfluff(cls, config_folder=None):
        if not config_folder:
            config_folder = cls.get_config_folder(mandatory=False)

        if config_folder:
            dbt_project_yaml_path = Path(config_folder.parent) / "dbt_project.yml"
        else:
            dbt_project_yaml_path = file_exists(Path(os.getcwd()), "dbt_project.yml")

        copier.run_auto(
            src_path=str(Path(__file__).parent.joinpath("components", "sqlfluff").resolve()),
            dst_path=str(dbt_project_yaml_path.parent.resolve()),
        )
