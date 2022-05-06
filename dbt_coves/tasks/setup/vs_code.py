import os
import questionary

from pathlib import Path
from rich.console import Console
from jinja2 import Environment, meta

from dbt_coves.config.config import DbtCovesConfig
from dbt_coves.utils.jinja import render_template
from dbt_coves.tasks.base import NonDbtBaseTask

from .dbt import SetupDbtTask
from .utils import print_row

console = Console()


class SetupVscodeTask(NonDbtBaseTask):
    """
    Task that runs ssh key generation, git repo clone and db connection setup
    """

    key_column_with = 50
    value_column_with = 30

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "vscode",
            parents=[base_subparser],
            help="Set up vscode settings for dbt-coves",
        )
        subparser.set_defaults(cls=cls, which="vscode")
        return subparser

    @classmethod
    def run(cls) -> int:
        workspace_path = os.environ.get("WORKSPACE_PATH", Path.cwd())
        config_folder = DbtCovesConfig.get_config_folder(
            workspace_path=workspace_path, mandatory=False
        )

        if not config_folder:
            print_row(
                "VSCode settings",
                f"settings weren't generated: .dbt_coves folder not found",
                new_section=True,
            )
            return 0

        template_path = Path(config_folder, "templates", "settings.json")
        if not template_path.exists():
            return

        code_local_path = Path(workspace_path, ".vscode", "settings.json")
        sqltools_status = "[red]MISSING[/red]"
        settings_exists = code_local_path.exists()
        if settings_exists:
            sqltools_status = "[green]FOUND :heavy_check_mark:[/green]"
        print_row(
            f"Checking for vs code settings",
            sqltools_status,
            new_section=True,
        )
        if not settings_exists:
            template_text = open(template_path, "r").read()
            print_row(" - settings.json template", "OK")

            env = Environment()
            parsed_content = env.parse(template_text)
            context = dict()
            for key in meta.find_undeclared_variables(parsed_content):
                if key not in context:
                    if "password" in key or "token" in key:
                        value = questionary.password(f"Please enter {key}:").ask()
                    else:
                        value = questionary.text(f"Please enter {key}:").ask()
                    context[key] = value
            new_settings = render_template(template_text, context)
            path = Path(workspace_path, ".vscode")
            path.mkdir(parents=True, exist_ok=True)

            with open(code_local_path, "w") as file:
                file.write(new_settings)
            console.print(
                f"[green]:heavy_check_mark: vs code settings successfully generated in {code_local_path}."
            )
        return 0
