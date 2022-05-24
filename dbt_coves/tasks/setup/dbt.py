import os
from pathlib import Path

import questionary
from jinja2 import Environment, meta
from rich.console import Console

from dbt_coves.config.config import DbtCovesConfig
from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.jinja import render_template
from dbt_coves.utils.shell import run_and_capture_cwd
from .utils import print_row, file_exists


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
        config_folder = cls.get_config_folder(mandatory=False)
        cls.dbt_init(config_folder)
        cls.dbt_debug(config_folder)
        cls.dbt_deps(config_folder)
        return 0

    @classmethod
    def get_config_folder(cls, mandatory=True):
        workspace_path = os.environ.get("WORKSPACE_PATH", Path.cwd())
        return DbtCovesConfig.get_config_folder(
            workspace_path=workspace_path, mandatory=mandatory
        )

    @classmethod
    def dbt_debug(cls, config_folder=None):
        if not config_folder:
            config_folder = cls.get_config_folder(mandatory=False)

        if config_folder:
            dbt_project_yaml_path = Path(config_folder.parent) / "dbt_project.yml"
        else:
            dbt_project_yaml_path = file_exists(Path(os.getcwd()), "dbt_project.yml")

        debug_status = "[red]FAIL[/red]"
        console.print("\n")

        output = run_and_capture_cwd(["dbt", "debug"], dbt_project_yaml_path.parent)

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
            config_folder = cls.get_config_folder(mandatory=False)

        if config_folder:
            dbt_project_yaml_path = Path(config_folder.parent) / "dbt_project.yml"
        else:
            dbt_project_yaml_path = file_exists(Path.cwd(), "dbt_project.yml")

        if not dbt_project_yaml_path:
            output = run_and_capture_cwd(["dbt", "init"], Path.cwd())

        else:
            init_status = (
                "[green]FOUND :heavy_check_mark:[/green] project already exists"
            )
            print_row(
                "dbt init",
                init_status,
                new_section=True,
            )
            output = run_and_capture_cwd(["dbt", "init"], dbt_project_yaml_path.parent)
        if output.returncode is 0:
            init_status = "[green]SUCCESS :heavy_check_mark:[/green]"
            print_row(
                "dbt init",
                init_status,
                new_section=True,
            )
        else:
            raise Exception("dbt init error. Check logs.")

    @classmethod
    def dbt_deps(cls, config_folder=None):
        if not config_folder:
            config_folder = cls.get_config_folder(mandatory=False)

        if config_folder:
            dbt_project_yaml_path = Path(config_folder.parent) / "dbt_project.yml"
        else:
            dbt_project_yaml_path = file_exists(Path(os.getcwd()), "dbt_project.yml")

        if dbt_project_yaml_path.exists():
            output = run_and_capture_cwd(["dbt", "deps"], dbt_project_yaml_path.parent)

            if output.returncode is 0:
                deps_status = "[green]SUCCESS :heavy_check_mark:[/green]"
            print_row(
                "dbt deps",
                deps_status,
                new_section=True,
            )
            if output.returncode > 0:
                raise Exception("dbt deps error. Check logs.")
        else:
            deps_status = (
                "[green]FOUND :heavy_check_mark:[/green] dbt project not found"
            )
            print_row(
                "dbt deps",
                deps_status,
                new_section=True,
            )
