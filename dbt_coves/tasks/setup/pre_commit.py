import os
from pathlib import Path

from rich.console import Console

from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.config.config import DbtCovesConfig
from dbt_coves.utils.jinja import render_template_file
from .utils import print_row

console = Console()


class SetupPrecommitTask(NonDbtBaseTask):
    """
    Task that runs ssh key generation, git repo clone and db connection setup
    """

    key_column_with = 50
    value_column_with = 30

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "precommit",
            parents=[base_subparser],
            help="Set up pre-commit for dbt-coves project",
        )
        subparser.add_argument(
            "--templates",
            type=str,
            help="Location of your sqlfluff, ci and pre-commit config files",
        )
        subparser.set_defaults(cls=cls, which="precommit")
        return subparser

    def run(self):
        workspace_path = workspace_path = os.environ.get("WORKSPACE_PATH", Path.cwd())
        config_folder = DbtCovesConfig.get_config_folder(
            workspace_path=workspace_path, mandatory=False
        )
        templates_folder = (
            self.get_config_value("templates") or f"{config_folder}/templates"
        )

        destination = Path(os.getcwd()) / ".pre-commit-config.yaml"

        precommit_dest_status = "[red]MISSING[/red]"
        precommit_exists = destination.exists()

        if precommit_exists:
            precommit_dest_status = "[green]FOUND :heavy_check_mark:[/green]"
        print_row(
            f"Checking for precommit settings",
            precommit_dest_status,
            new_section=True,
        )

        if not precommit_exists:
            context = {
                "relation": "",
                "columns": None,
                "nested": {},
                "adapter_name": None,
            }
            render_template_file(
                ".pre-commit-config.yaml",
                context,
                destination,
                templates_folder,
            )
            console.print(f"Pre-commit installed at [green]{destination}[/green]")

        return 0

    def get_config_value(self, key):
        return self.coves_config.integrated["setup"]["precommit"][key]
