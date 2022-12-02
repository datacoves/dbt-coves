import os
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path

from rich.console import Console
from rich.text import Text

from dbt_coves.tasks.base import NonDbtBaseConfiguredTask

console = Console()

class RunDbtException(Exception):
    pass

class RunDbtTask(NonDbtBaseConfiguredTask):
    """
    Task that executes dbt on an isolated/prepared environment
    """

    arg_parser = None

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        ext_subparser = sub_parsers.add_parser(
            "dbt",
            parents=[base_subparser],
            # help="Run dbt on an isolated environment",
            help="Use this command to run dbt commands on special environments such as Airflow, or CI workers.",
        )
        ext_subparser.set_defaults(cls=cls, which="dbt")
        cls.arg_parser = ext_subparser
        ext_subparser.add_argument(
            "--virtualenv",
            type=str,
            help="Virtual environment variable or path. i.e.: AIRFLOW__VIRTUALENV_PATH or /opt/user/virtualenvs/airflow",
        )
        ext_subparser.add_argument(
            "command",
            type=str,
            nargs="+",
            help='dbt command to run, i.e. "run -s model_name"',
        )

        return ext_subparser

    def run(self) -> int:
        project_dir = self.get_config_value("project_dir")
        if not project_dir:
            project_dir = os.environ.get("DBT_PROJECT_DIR", os.environ.get("DBT_HOME"))
        if not project_dir:
            console.print("[red]No dbt project specified[/red].")
            return -1

        command = self.get_config_value("command")
        if self.is_readonly(project_dir):
            tmp_dir = tempfile.NamedTemporaryFile().name
            console.print(
                f"Readonly project detected. Copying it to temp directory [b]{tmp_dir}[/b]"
            )
            subprocess.run(["cp", "-rf", f"{project_dir}/", tmp_dir], check=False)
            try:
                self.run_dbt(command, cwd=tmp_dir)
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)
        else:
            self.run_dbt(command, cwd=project_dir)

        return 0

    def run_dbt(self, args: list, cwd: str):
        """Run dbt command on a specific directory passing received arguments. Runs dbt deps if missing packages"""
        if not os.path.exists(os.path.join(cwd, "dbt_packages")) and not os.path.exists(
            os.path.join(cwd, "dbt_modules")
        ):
            console.print(f"[red]Missing dbt packages[/red]")
            self.run_command(f"dbt deps", cwd=cwd)
        str_args = " ".join([arg if " " not in arg else f"'{arg}'" for arg in args])
        self.run_command(f"dbt {str_args}", cwd=cwd)

    def is_readonly(self, folder: str) -> bool:
        """Returns True if `folder` is readonly"""
        stat = os.statvfs(folder)
        return bool(stat.f_flag & os.ST_RDONLY) or not os.access(folder, os.W_OK)

    def run_command(
        self,
        command: str,
        cwd=None,
    ):
        """Activates a python environment if found and runs a command using it"""
        env_path = None
        virtualenv = self.get_config_value("virtualenv")
        env = os.environ.copy()
        if virtualenv:
            # Ensure it's a Path to avoid
            # conflicts with trailing / at later concatenation
            env_path = Path(os.environ.get(virtualenv, virtualenv))
        if env_path and env_path.exists():
            cmd_list = shlex.split(
                f"/bin/bash -c 'source {env_path}/bin/activate && {command}'"
            )
        else:
            cmd_list = shlex.split(command)
            
        try:
            output = subprocess.check_output(cmd_list, env=env, cwd=cwd)
            console.print(f"{Text.from_ansi(output.decode())}\n"\
                f"[green]{command} :heavy_check_mark:[/green]")
        except subprocess.CalledProcessError as e:
            raise RunDbtException(f"Exception ocurred running [red]{command}[/red]:\n"\
                 f"{Text.from_ansi(e.output.decode())}")

    def get_config_value(self, key):
        return self.coves_config.integrated["dbt"][key]
