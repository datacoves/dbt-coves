import os
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path

from rich.console import Console
from rich.text import Text

from dbt_coves.tasks.base import NonDbtBaseConfiguredTask
from dbt_coves.utils.tracking import trackable

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
            help="""Use this command to run dbt commands on special environments
            such as Airflow, or CI workers. When a read-write copy needs to be
            created, its path can be found in /tmp/dbt_coves_dbt_clone_path.txt.""",
        )
        ext_subparser.set_defaults(cls=cls, which="dbt")
        cls.arg_parser = ext_subparser
        ext_subparser.add_argument(
            "--virtualenv",
            type=str,
            help="""Path to virtual environment where dbt commands
            will be executed. i.e.: /opt/user/virtualenvs/airflow""",
        )
        ext_subparser.add_argument(
            "--cleanup",
            action="store_true",
            default=False,
            help="If a read-write clone is created, remove it after completion",
        )
        ext_subparser.add_argument(
            "command",
            type=str,
            nargs="+",
            help="dbt command to run, i.e. 'run -s model_name'",
        )

        return ext_subparser

    @trackable
    def run(self) -> int:
        project_dir = self.get_config_value("project_dir")
        if not project_dir:
            project_dir = os.environ.get("DBT_PROJECT_DIR", os.environ.get("DATACOVES__DBT_HOME"))
        if not project_dir:
            console.print("[red]No dbt project specified[/red].")
            return -1

        command = self.get_config_value("command")
        if self.is_readonly(project_dir):
            path_file = Path("/tmp/dbt_coves_dbt_clone_path.txt")
            tmp_dir = None
            if path_file.exists():
                tmp_dir = open(path_file).read().rstrip("\n")
            if not tmp_dir:
                tmp_dir = tempfile.NamedTemporaryFile().name
            if not os.path.exists(tmp_dir):
                console.print(
                    f"Readonly project detected. Copying it to temp directory [b]{tmp_dir}[/b]."
                )
                subprocess.run(["cp", "-rf", f"{project_dir}/", tmp_dir], check=False)
            try:
                self.run_dbt(command, cwd=tmp_dir)
            finally:
                if self.get_config_value("cleanup"):
                    console.print("Removing cloned read-write copy.")
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                else:
                    console.print("Writing dbt clone path to '/tmp/dbt_coves_dbt_clone_path.txt'.")
                    with open(path_file, "w") as f:
                        f.write(tmp_dir)
        else:
            self.run_dbt(command, cwd=project_dir)

        return 0

    def run_dbt(self, args: list, cwd: str):
        """
        Run dbt command on a specific directory passing received arguments.
        Runs dbt deps if missing packages
        """
        if not os.path.exists(os.path.join(cwd, "dbt_packages")) and not os.path.exists(
            os.path.join(cwd, "dbt_modules")
        ):
            console.print("[red]Missing dbt packages[/red]")
            self.run_command("dbt deps", cwd=cwd)
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
            cmd_list = shlex.split(f"/bin/bash -c 'source {env_path}/bin/activate && {command}'")
        else:
            cmd_list = shlex.split(command)

        try:
            output = subprocess.check_output(cmd_list, env=env, cwd=cwd, stderr=subprocess.PIPE)
            console.print(
                f"{Text.from_ansi(output.decode())}\n"
                f"[green]{command} :heavy_check_mark:[/green]"
            )
        except subprocess.CalledProcessError as e:
            formatted = f"{Text.from_ansi(e.stderr.decode()) if e.stderr else Text.from_ansi(e.stdout.decode())}"
            e.stderr = f"An error has occurred running [red]{command}[/red]:\n{formatted}"
            raise

    def get_config_value(self, key):
        return self.coves_config.integrated["dbt"][key]
