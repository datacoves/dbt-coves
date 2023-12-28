import json
import os
import shlex
import subprocess
from os import environ
from pathlib import Path
from typing import Any, Dict, Set


class GeneratorException(Exception):
    pass


class BaseDbtGenerator:
    """
    Common functionalities for all dbt-related Generators
    """

    def __init__(
        self,
        dbt_project_path: str = "",
        virtualenv_path: str = "",
        run_dbt_compile: bool = False,
        run_dbt_deps: bool = False,
        dbt_list_args: str = "",
    ) -> None:
        self.dbt_project_path = dbt_project_path
        self.virtualenv_path = virtualenv_path
        self.run_dbt_compile = run_dbt_compile
        self.run_dbt_deps = run_dbt_deps
        self.dbt_list_args = dbt_list_args

    def discover_dbt_connections(self) -> Set[str]:
        """
        Discover DBT source(s)' Airbyte/Fivetran connection IDs based on params
        """
        if self.virtualenv_path:
            self.virtualenv_path = Path(f"{self.virtualenv_path}/bin/activate").absolute()

        if Path(self.dbt_project_path).is_absolute():
            self.dbt_project_path = Path(self.dbt_project_path)
        else:
            self.dbt_project_path = (
                Path(environ.get("DATACOVES__REPO_PATH", "/config/workspace"))
                / self.dbt_project_path
            )
        cwd = self.dbt_project_path

        deploy_path = None
        if self.is_readonly(self.dbt_project_path):
            commit = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True,
                text=True,
                cwd=self.dbt_project_path,
            ).stdout.strip("\n")
            deploy_path = "/tmp/airbyte-generator-" + commit
            # Move folders
            subprocess.run(["cp", "-rf", self.dbt_project_path, deploy_path], check=True)
            cwd = deploy_path

        try:
            if self.run_dbt_deps:
                if self.virtualenv_path:
                    command = self.get_bash_command(self.virtualenv_path, "dbt deps")
                else:
                    command = ["dbt", "deps"]
                subprocess.run(
                    command,
                    check=True,
                    cwd=cwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

            if self.run_dbt_compile:
                if self.virtualenv_path:
                    command = self.get_bash_command(
                        self.virtualenv_path, f"dbt compile {self.dbt_list_args}"
                    )
                else:
                    command = ["dbt", "compile"] + self.dbt_list_args.split()
                subprocess.run(
                    command,
                    check=True,
                    cwd=cwd,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )

            if self.virtualenv_path:
                command = self.get_bash_command(
                    self.virtualenv_path, f"dbt ls --resource-type source {self.dbt_list_args}"
                )
            else:
                command = [
                    "dbt",
                    "ls",
                    "--resource-type",
                    "source",
                ] + self.dbt_list_args.split()

            process = subprocess.run(
                command,
                cwd=cwd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=True,
            )
            stdout = process.stdout.decode()
        except subprocess.CalledProcessError as e:
            error_message = ""
            if e.stdout:
                error_message += f"{e.stdout.decode()}\n"
            if e.stderr:
                error_message += f"{e.stderr.decode()}"
            raise GeneratorException(f"Exception ocurred running {command}\n{error_message}")

        sources_list = []
        if "No nodes selected" not in stdout:
            sources_list = [
                src.replace("source:", "source.")
                for src in stdout.split("\n")
                if (src and "source:" in src)
            ]
        manifest_json = json.load(open(Path(cwd) / "target" / "manifest.json"))

        if deploy_path:
            subprocess.run(["rm", "-rf", deploy_path], check=True)

        connections_ids = []
        for source in sources_list:
            # Transform the 'dbt source' into [db, schema, table]
            source_table = manifest_json["sources"][source]["identifier"].lower()
            if source_table in self.ignored_source_tables:
                continue
            source_db = manifest_json["sources"][source]["database"].lower()
            source_schema = manifest_json["sources"][source]["schema"].lower()
            connections_for_source = self.get_pipeline_connection_ids(
                source_db, source_schema, source_table
            )
            if connections_for_source:
                for connection in connections_for_source:
                    if connection not in connections_ids:
                        connections_ids.append(connection)

        return connections_ids


class BaseDbtCovesTaskGenerator:
    """
    Common functionalities for all Generators
    """

    def get_bash_command(self, virtualenv_path, command):
        return shlex.split(f"/bin/bash -c 'source {virtualenv_path} && {command}'")

    def generate_task(self, name: str, operator: str, **kwargs: Dict[str, Any]) -> str:
        """
        Common `generate_task` for all Generators
        Receive task_id, connection_id and Operator-string
        Returns Airflow call as a string
        """
        func_call = ", ".join(
            f'{k}="{v}"' if isinstance(v, str) else f"{k}={v}" for k, v in kwargs.items()
        )
        return f"{name} = {operator}({func_call})"

    def is_readonly(self, folder: str) -> bool:
        """Returns True if `folder` is readonly"""
        stat = os.statvfs(folder)
        return bool(stat.f_flag & os.ST_RDONLY) or not os.access(folder, os.W_OK)
