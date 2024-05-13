"""
Task that helps Datacoves users set-up their Environment.
On a first stage, it'll generate:
- automate/dbt/profiles.yml
- orchestrate/dags/sample_dag.py
- .github/workflows/push_to_main.yml
"""

import os
from pathlib import Path

import copier

from dbt_coves.core.exceptions import DbtCovesException
from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.tracking import trackable

SERVICES = {
    "Airflow dbt profile": "airflow_dbt_profile",
    "Airflow sample DAG": "airflow_sample_dag",
    "Github CI workflow": "github_workflow",
}


class SetupDatacovesTask(NonDbtBaseTask):
    """
    Task that runs pre-commit setup
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "datacoves",
            parents=[base_subparser],
            help="Set up Datacoves CI, profiles and DAGs structure",
        )
        subparser.set_defaults(cls=cls, which="datacoves")
        return subparser

    @trackable
    def run(self) -> int:
        try:
            self.repo_path = os.environ["DATACOVES__REPO_PATH"]
            self.dbt_home = os.environ["DATACOVES__DBT_HOME"]
            self.copier_context = {"datacoves_env": True}
        except KeyError:
            raise DbtCovesException(
                "This command is meant to be run in a Datacoves environment only"
            )
        return self.setup_datacoves()

    def _get_path_rel_to_root(self, path):
        return str(Path(path).relative_to(self.repo_path))

    def setup_datacoves(self):
        # dbt profile data gathering

        airflow_profile_path = os.environ.get(
            "DATACOVES__AIRFLOW_DBT_PROFILE_PATH", f"{self.dbt_home}/automate/dbt"
        )
        self.copier_context["airflow_profile_path"] = self._get_path_rel_to_root(
            airflow_profile_path
        )
        self.copier_context["dbt_adapter"] = os.environ.get("DATACOVES__DBT_ADAPTER", "default")
        # sample DAG data
        airflow_dags_path = os.environ.get(
            "DATACOVES__AIRFLOW_DAGS_PATH", f"{self.dbt_home}/orchestrate/dags"
        )
        self.copier_context["airflow_dags_path"] = self._get_path_rel_to_root(airflow_dags_path)
        copier.run_auto(
            src_path=str(Path(__file__).parent.joinpath("templates", "datacoves").resolve()),
            dst_path=self.repo_path,
            data=self.copier_context,
        )
        return 0
