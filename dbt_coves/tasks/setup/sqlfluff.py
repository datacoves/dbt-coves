import os
import glob
from pathlib import Path

from rich.console import Console

from dbt_coves.config.config import DbtCovesConfig
from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.jinja import render_template_file
from .utils import print_row, file_exists

console = Console()


class SetupSqlfluffTask(NonDbtBaseTask):
    """
    Task that runs ssh key generation, git repo clone and db connection setup
    """

    KEY_COLUMN_WIDTH = 20
    VALUE_COLUMN_WIDTH = 50
    arg_parser = None

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "sqlfluff",
            parents=[base_subparser],
            help="Set up sqlfluff for dbt-coves project",
        )
        subparser.add_argument(
            "--templates",
            type=str,
            help="Location of your sqlfluff, ci and pre-commit config files",
        )
        subparser.set_defaults(cls=cls, which="sqlfluff")
        cls.arg_parser = base_subparser
        return subparser

    def run(self) -> int:
        dbt_project_path = False
        workspace_path = workspace_path = os.environ.get("WORKSPACE_PATH", Path.cwd())
        config_folder = DbtCovesConfig.get_config_folder(
            workspace_path=workspace_path, mandatory=False
        )
        templates_folder = (
            self.get_config_value("templates") or f"{config_folder}/templates"
        )

        execution_path = Path(os.getcwd())

        dbt_project_dest_status = "[red]MISSING[/red]"
        sqlfluff_dest_status = "[red]MISSING[/red]"
        sqlfluff_file = file_exists(execution_path, ".sqlfluff")

        if sqlfluff_file:
            sqlfluff_dest_status = (
                f"[green]FOUND: {sqlfluff_file} :heavy_check_mark:[/green]"
            )
        print_row(
            f"Checking for sqlfluff settings",
            sqlfluff_dest_status,
            new_section=False,
            KEY_COLUMN_WIDTH=40,
            VALUE_COLUMN_WIDTH=100,
        )

        if not sqlfluff_file:
            dbt_project_path = file_exists(execution_path, "dbt_project.yml")
            if dbt_project_path:
                execution_path = Path(dbt_project_path).parent
                dbt_project_dest_status = (
                    f"[green]FOUND: {execution_path} :heavy_check_mark:[/green]"
                )
            print_row(
                f"Checking for dbt project existence",
                dbt_project_dest_status,
                new_section=False,
                KEY_COLUMN_WIDTH=40,
                VALUE_COLUMN_WIDTH=100,
            )
            if not dbt_project_path:
                console.print(
                    f"Could not find sqlfluff or existent dbt project. Please initialize a dbt project before installing sqlfluff"
                )
                return 1

            destination_path = execution_path / ".sqlfluff"
            context = {
                "relation": "",
                "columns": None,
                "nested": {},
                "adapter_name": None,
            }
            render_template_file(
                ".sqlfluff",
                context,
                destination_path,
                templates_folder,
            )
            console.print(f"Sqlfluff installed at [green]{destination_path}[/green]")

        return 0

    def get_config_value(self, key):
        return self.coves_config.integrated["setup"]["sqlfluff"][key]
