import os
from pathlib import Path

import copier
import questionary
from rich.console import Console

from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.tracking import trackable

from .utils import get_dbt_projects

AVAILABLE_SERVICES = {
    "dbt profile for automated runs": "setup_dbt_profile",
    "Initial CI/CD scripts": "setup_ci_cd",
    "Linting with SQLFluff, dbt-checkpoint and/or YMLLint": "setup_precommit",
    "Sample Airflow DAG": "setup_airflow_dag",
}

console = Console()


class DbtCovesSetupException(Exception):
    pass


class SetupTask(NonDbtBaseTask):
    """
    Task that code-gen dbt resources
    """

    key_column_with = 20
    value_column_with = 50
    arg_parser = None

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        ext_subparser = sub_parsers.add_parser(
            "setup",
            parents=[base_subparser],
            help="Set up project components (sqlfluff, CI, pre-commit, etc)",
        )
        ext_subparser.add_argument(
            "--no-prompt",
            action="store_true",
            help="Generate all Datacoves components without prompting for confirmation",
            default=False,
        )
        ext_subparser.set_defaults(cls=cls, which="setup")
        cls.arg_parser = ext_subparser
        return ext_subparser

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_config_value(self, key):
        return self.coves_config.integrated["setup"][key]

    @trackable
    def run(self) -> int:
        self.repo_path = os.environ.get("DATACOVES__REPO_PATH", "/config/workspace")
        self.copier_context = {"no_prompt": self.get_config_value("no_prompt")}
        return self.setup_datacoves()

    def _get_path_rel_to_root(self, path):
        return str(Path(path).relative_to(self.repo_path))

    def setup_datacoves(self):
        # dbt profile data gathering
        choices = questionary.checkbox(
            "What services would you like to set up?",
            choices=list(AVAILABLE_SERVICES.keys()),
        ).ask()
        services = [AVAILABLE_SERVICES[service] for service in choices]

        airflow_profile_path = os.environ.get(
            "DATACOVES__AIRFLOW_DBT_PROFILE_PATH", f"{self.repo_path}/automate/dbt"
        )
        if not airflow_profile_path:
            airflow_profile_path = f"{self.repo_path}/automate/dbt"

        self.copier_context["airflow_profile_path"] = self._get_path_rel_to_root(
            airflow_profile_path
        )

        dbt_adapter = os.environ.get("DATACOVES__DBT_ADAPTER")
        if dbt_adapter:
            self.copier_context["dbt_adapter"] = dbt_adapter

        # sample DAG data
        airflow_dags_path = os.environ.get(
            "DATACOVES__AIRFLOW_DAGS_PATH", f"{self.repo_path}/orchestrate/dags"
        )

        self.copier_context["airflow_dags_path"] = self._get_path_rel_to_root(airflow_dags_path)
        if "setup_precommit" in services:
            dbt_project_paths = get_dbt_projects(self.repo_path)
            if not dbt_project_paths:
                console.print(
                    "Your repository doesn't contain any dbt project where to install [red]pre-commit[/red] into"
                )
                services.remove("setup_precommit")
            elif len(dbt_project_paths) == 1:
                self.copier_context["dbt_project_dir"] = dbt_project_paths[0]
            else:
                self.copier_context["dbt_project_dir"] = questionary.select(
                    "In which dbt project would you like to install pre-commit?",
                    choices=dbt_project_paths,
                ).ask()
        self.copier_context["services"] = services
        copier.run_auto(
            src_path=str(Path(__file__).parent.joinpath("templates", "datacoves").resolve()),
            dst_path=self.repo_path,
            data=self.copier_context,
        )
        return 0
