import os
from pathlib import Path

import copier
import questionary
import requests
from rich.console import Console

from dbt_coves import __dbt_major_version__, __dbt_minor_version__, __dbt_patch_version__
from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.tracking import trackable

from .utils import get_dbt_projects

AVAILABLE_SERVICES = {
    "Base dbt project": "setup_dbt_project",
    "dbt profile for automated runs": "setup_dbt_profile",
    "Initial CI/CD scripts": "setup_ci_cd",
    "Linting with SQLFluff, dbt-checkpoint and/or YMLLint": "setup_precommit",
    "Sample Airflow DAGs": "setup_airflow_dag",
    "dbt-coves' config and/or templates": "setup_dbt_coves",
}

THIRD_PARTY_PRECOMMIT_REPOS = {
    "dbt_checkpoint": "https://api.github.com/repos/dbt-checkpoint/dbt-checkpoint/tags",
    "yamllint": "https://api.github.com/repos/adrienverge/yamllint/tags",
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
            help="Set up dbt project components (dbt project, CI, pre-commit, Airflow DAGs)",
        )
        ext_subparser.add_argument(
            "--no-prompt",
            action="store_true",
            help="Generate all Datacoves components without prompting for confirmation",
            default=False,
        )
        ext_subparser.add_argument(
            "--quiet",
            action="store_true",
            help="Skip rendering results",
            default=False,
        )
        ext_subparser.add_argument(
            "--template-url",
            help="URL to the setup template repository",
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
        self.repo_path = os.environ.get("DATACOVES__REPO_PATH", Path().resolve())
        self.copier_context = {"no_prompt": self.get_config_value("no_prompt")}
        self.copier_context[
            "dbt_core_version"
        ] = f"{__dbt_major_version__}.{__dbt_minor_version__}.{__dbt_patch_version__}"
        self.copier_context[
            "dbt_adapter_version"
        ] = f"{__dbt_major_version__}.{__dbt_minor_version__}"
        return self.setup_datacoves()

    def _get_latest_repo_tag(self, repo_url):
        res = requests.get(f"{repo_url}")
        if res.status_code != 200:
            return None
        tags = res.json()
        return tags[0].get("name")

    def setup_datacoves(self):
        choices = questionary.checkbox(
            "What services would you like to set up?",
            choices=list(AVAILABLE_SERVICES.keys()),
        ).ask()
        services = [AVAILABLE_SERVICES[service] for service in choices]
        # dbt project
        dbt_projects = get_dbt_projects(self.repo_path)
        if not dbt_projects:
            if "setup_dbt_project" in services:
                project_dir = questionary.select(
                    "Where should the dbt project be created?",
                    choices=["current directory", "transform"],
                ).ask()
                if "current directory" in project_dir:
                    project_dir = "."
                self.copier_context["dbt_project_dir"] = project_dir
                self.copier_context["dbt_project_name"] = questionary.text(
                    "What is the name of the dbt project?"
                ).ask()
            elif "setup_precommit" in services:
                raise DbtCovesSetupException(
                    "No dbt project found in the current directory. "
                    "Please create one before setting up dbt components."
                )
            else:
                self.copier_context["dbt_project_dir"] = "."
            self.copier_context["is_new_project"] = True
        elif len(dbt_projects) == 1:
            self.copier_context["dbt_project_dir"] = dbt_projects[0].get("path")
            self.copier_context["dbt_project_name"] = dbt_projects[0].get("name")
        else:
            project_dir = questionary.select(
                "In which dbt project would you like to perform setup?",
                choices=[prj.get("path") for prj in dbt_projects],
            ).ask()
            self.copier_context["dbt_project_dir"] = project_dir
            self.copier_context["dbt_project_name"] = [
                prj.get("name") for prj in dbt_projects if prj.get("path") == project_dir
            ][0]

        # dbt profile data gathering
        airflow_profile_path = os.environ.get("DATACOVES__AIRFLOW_DBT_PROFILE_PATH", "automate/dbt")
        if not airflow_profile_path:
            airflow_profile_path = "automate/dbt"
        self.copier_context["airflow_profile_path"] = airflow_profile_path

        dbt_adapter = os.environ.get("DATACOVES__DBT_ADAPTER")
        if dbt_adapter:
            self.copier_context["datacoves_dbt_adapter"] = dbt_adapter
        else:
            self.copier_context["datacoves_dbt_adapter"] = False

        # sample DAG data
        if "setup_airflow_dag" in services:
            dags_path = os.environ.get("DATACOVES__AIRFLOW_DAGS_PATH")
            if not dags_path:
                self.copier_context["airflow_dags_confirm_path"] = True
                self.copier_context["tentative_dags_path"] = "orchestrate/dags"
            else:
                self.copier_context["dags_path"] = dags_path

            yml_dags_path = os.environ.get("DATACOVES__AIRFLOW_DAGS_YML_PATH")
            if not yml_dags_path:
                self.copier_context["yml_dags_confirm_path"] = True
                self.copier_context["tentative_yml_dags_path"] = "orchestrate/dags_yml_definitions"
            else:
                self.copier_context["yml_dags_path"] = yml_dags_path
        for service in services:
            self.copier_context[service] = True

        if "setup_precommit" in services:
            dbt_checkpoint_version = self._get_latest_repo_tag(
                THIRD_PARTY_PRECOMMIT_REPOS["dbt_checkpoint"]
            )
            if dbt_checkpoint_version:
                self.copier_context["dbt_checkpoint_version"] = dbt_checkpoint_version
            else:
                self.copier_context["ask_dbt_checkpoint_version"] = True
            yamllint_version = self._get_latest_repo_tag(THIRD_PARTY_PRECOMMIT_REPOS["yamllint"])
            if yamllint_version:
                self.copier_context["yamllint_version"] = yamllint_version
            else:
                self.copier_context["ask_yamllint_version"] = True
            self.copier_context["sqlfluff_version"] = os.environ.get(
                "DATACOVES__SQLFLUFF_VERSION", "3.1.1"
            )

        if "setup_ci_cd" in services:
            self.copier_context["datacoves_env_version"] = os.environ.get(
                "DATACOVES__VERSION_MAJOR_MINOR__ENV", "3"
            )

        copier.run_auto(
            src_path=self.get_config_value("template_url"),
            dst_path=self.repo_path,
            data=self.copier_context,
            quiet=self.get_config_value("quiet"),
        )
        return 0
