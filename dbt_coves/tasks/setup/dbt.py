import os
from pathlib import Path

import questionary
from jinja2 import Environment, meta
from rich.console import Console

from dbt_coves.config.config import DbtCovesConfig
from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.jinja import render_template
from dbt_coves.utils.shell import run, run_and_capture_cwd
from .utils import print_row


console = Console()


class SetupDbtTask(NonDbtBaseTask):
    """
    Task that runs ssh key generation, git repo clone and db connection setup
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "dbt",
            parents=[base_subparser],
            help="Set up dbt for dbt-coves project",
        )
        subparser.set_defaults(cls=cls, which="dbt")
        return subparser

    @classmethod
    def run(cls) -> int:
        config_folder = cls.get_config_folder()
        context = cls.get_dbt_profiles_context(config_folder)
        cls.dbt_init(config_folder)
        cls.dbt_debug(config_folder)
        return 0

    @classmethod
    def get_config_folder(cls):
        workspace_path = os.environ.get("WORKSPACE_PATH", Path.cwd())
        return DbtCovesConfig.get_config_folder(workspace_path=workspace_path)

    @classmethod
    def get_dbt_profiles_context(cls, config_folder=None):
        if not config_folder:
            config_folder = cls.get_config_folder()
        profiles_status = "[red]MISSING[/red]"
        default_dbt_path = Path("~/.dbt").expanduser()
        dbt_path = os.environ.get("DBT_PROFILES_DIR", default_dbt_path)
        profiles_path = Path(dbt_path, "profiles.yml")
        profiles_exists = profiles_path.exists()
        if profiles_exists:
            profiles_status = "[green]FOUND :heavy_check_mark:[/green]"
        print_row(
            f"Checking for profiles.yml in '{dbt_path}'",
            profiles_status,
            new_section=True,
        )

        if profiles_exists:
            return None

        template_path = Path(config_folder, "templates", "profiles.yml")
        try:
            template_text = open(template_path, "r").read()
            print_row(" - profiles.yml template", "OK")
        except FileNotFoundError:
            raise Exception(
                f"Could not genereate Dbt profile. Template not found in '{template_path}'"
            )

        env = Environment()
        parsed_content = env.parse(template_text)
        context = dict()
        for key in meta.find_undeclared_variables(parsed_content):
            if "password" in key or "token" in key:
                value = questionary.password(f"Please enter {key}:").ask()
            else:
                value = questionary.text(f"Please enter {key}:").ask()
            context[key] = value
        new_profiles = render_template(template_text, context)
        profiles_path.parent.mkdir(parents=True, exist_ok=True)
        with open(profiles_path, "w") as file:
            file.write(new_profiles)
        console.print(
            f"[green]:heavy_check_mark: dbt profiles successfully generated in {profiles_path}."
        )
        return context

    @classmethod
    def dbt_debug(cls, config_folder=None):
        if not config_folder:
            config_folder = cls.get_config_folder()
        debug_status = "[red]FAIL[/red]"
        console.print("\n")

        output = run(["dbt", "debug"], cwd=config_folder.parent)

        if output.returncode is 0:
            debug_status = "[green]SUCCESS :heavy_check_mark:[/green]"
        print_row(
            "dbt debug",
            debug_status,
            new_section=True,
        )
        if output.returncode > 0:
            raise Exception("dbt debug error. Check logs.")

    @classmethod
    def dbt_init(cls, config_folder=None):
        if not config_folder:
            config_folder = cls.get_config_folder()
        dbt_project_yaml_path = Path(config_folder.parent) / "dbt_project.yml"

        if not dbt_project_yaml_path.exists():
            output = run_and_capture_cwd(["dbt", "init"], cwd=config_folder.parent)

            if output.returncode is 0:
                init_status = "[green]SUCCESS :heavy_check_mark:[/green]"
            print_row(
                "dbt init",
                init_status,
                new_section=True,
            )
            if output.returncode > 0:
                raise Exception("dbt init error. Check logs.")
        else:
            init_status = (
                "[green]FOUND :heavy_check_mark:[/green] project already exists"
            )
            print_row(
                "dbt init",
                init_status,
                new_section=True,
            )

    @classmethod
    def dbt_deps(cls, config_folder=None):
        if not config_folder:
            config_folder = cls.get_config_folder()
        dbt_project_yaml_path = Path(config_folder.parent) / "dbt_project.yml"

        if dbt_project_yaml_path.exists():
            output = run_and_capture_cwd(["dbt", "deps"], cwd=config_folder.parent)

            if output.returncode is 0:
                init_status = "[green]SUCCESS :heavy_check_mark:[/green]"
            print_row(
                "dbt deps",
                init_status,
                new_section=True,
            )
            if output.returncode > 0:
                raise Exception("dbt deps error. Check logs.")
        else:
            init_status = (
                "[green]FOUND :heavy_check_mark:[/green] dbt project not found"
            )
            print_row(
                "dbt deps",
                init_status,
                new_section=True,
            )
