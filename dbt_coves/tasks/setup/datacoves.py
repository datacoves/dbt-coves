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
        subparser.add_argument(
            "--no-prompt",
            action="store_true",
            help="Generate all Datacoves components without prompting for confirmation",
            default=False,
        )
        subparser.set_defaults(cls=cls, which="datacoves")
        return subparser

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_config_value(self, key):
        return self.coves_config.integrated["setup"]["datacoves"][key]

    @trackable
    def run(self) -> int:
        self.repo_path = os.environ.get("DATACOVES__REPO_PATH", "/config/workspace")
        self.copier_context = {"no_prompt": self.get_config_value("no_prompt")}
        return self.setup_datacoves()

    def _get_path_rel_to_root(self, path):
        return str(Path(path).relative_to(self.repo_path))

    def setup_datacoves(self):
        # dbt profile data gathering
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
        copier.run_auto(
            src_path=str(Path(__file__).parent.joinpath("templates", "datacoves").resolve()),
            dst_path=self.repo_path,
            data=self.copier_context,
        )
        return 0
