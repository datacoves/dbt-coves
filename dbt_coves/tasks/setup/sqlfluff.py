import os
from pathlib import Path

from rich.console import Console

from dbt_coves.config.config import DbtCovesConfig
from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.jinja import render_template_file
from .utils import print_row

console = Console()


class SetupSqlfluffTask(NonDbtBaseTask):
    """
    Task that runs ssh key generation, git repo clone and db connection setup
    """

    key_column_with = 50
    value_column_with = 30

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "sqlfluff",
            parents=[base_subparser],
            help="Initialises dbt project, sets up SSH keys, git repo, and db connections.",
        )
        subparser.add_argument(
            "--templates",
            type=str,
            help="Location of your sqlfluff, ci and pre-commit config files",
        )
        subparser.set_defaults(cls=cls, which="sqlfluff")
        return subparser

    def run(self) -> int:
        workspace_path = workspace_path = os.environ.get("WORKSPACE_PATH", Path.cwd())
        config_folder = DbtCovesConfig.get_config_folder(workspace_path=workspace_path)
        templates_folder = (
            self.get_config_value("templates") or f"{config_folder}/templates"
        )

        destination_path = Path(os.getcwd())
        sqlfluff_dest_path = destination_path / ".sqlfluff"
        sqlfluffignore_dest_path = destination_path / ".sqlfluffignore"

        sqlfluff_dest_status = "[red]MISSING[/red]"
        sqlfluff_exists = (
            sqlfluff_dest_path.exists() and sqlfluffignore_dest_path.exists()
        )
        if sqlfluff_exists:
            sqlfluff_dest_status = "[green]FOUND :heavy_check_mark:[/green]"
        print_row(
            f"Checking for sqlfluff settings",
            sqlfluff_dest_status,
            new_section=True,
        )

        if not sqlfluff_exists:
            context = {
                "relation": "",
                "columns": None,
                "nested": {},
                "adapter_name": None,
            }
            render_template_file(
                ".sqlfluff",
                context,
                sqlfluff_dest_path,
                templates_folder,
            )
            render_template_file(
                ".sqlfluffignore",
                context,
                sqlfluffignore_dest_path,
                templates_folder,
            )
            console.print(f"Sqlfluff installed at [green]{destination_path}[/green]")

        return 0

    def get_config_value(self, key):
        return self.coves_config.integrated["setup"]["sqlfluff"][key]
