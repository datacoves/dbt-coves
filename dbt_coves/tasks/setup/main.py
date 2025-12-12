import os
from pathlib import Path

import copier
import requests
from jinja2.exceptions import TemplateSyntaxError
from rich.console import Console

from dbt_coves import __dbt_major_version__, __dbt_minor_version__, __dbt_patch_version__
from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.tracking import trackable

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
        ext_subparser.add_argument(
            "--update",
            action="store_true",
            help="Update the existing Datacoves components",
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
        self.repo_path = os.environ.get("DATACOVES__REPO_PATH", Path().resolve())
        self.copier_context = {"no_prompt": self.get_config_value("no_prompt")}
        self.copier_context["dbt_core_version"] = (
            f"{__dbt_major_version__}.{__dbt_minor_version__}.{__dbt_patch_version__}"
        )
        self.copier_context["dbt_adapter_version"] = (
            f"{__dbt_major_version__}.{__dbt_minor_version__}"
        )
        return self.setup_datacoves()

    def _get_latest_repo_tag(self, repo_url):
        res = requests.get(f"{repo_url}")
        if res.status_code != 200:
            return None
        tags = res.json()
        return tags[0].get("name")

    def setup_datacoves(self):
        dbt_checkpoint_version = self._get_latest_repo_tag(
            THIRD_PARTY_PRECOMMIT_REPOS["dbt_checkpoint"]
        )
        if dbt_checkpoint_version:
            os.environ["DATACOVES__DBT_CHECKPOINT_VERSION"] = dbt_checkpoint_version
        yamllint_version = self._get_latest_repo_tag(THIRD_PARTY_PRECOMMIT_REPOS["yamllint"])
        if yamllint_version:
            os.environ["DATACOVES__YAMLLINT_VERSION"] = yamllint_version
        self.copier_context["datacoves_env_version"] = os.environ.get(
            "DATACOVES__VERSION_MAJOR_MINOR__ENV", "3"
        )
        dbt_home = os.environ.get("DATACOVES__DBT_HOME")
        dbt_home_relpath = Path(dbt_home).relative_to(self.repo_path)
        os.environ["DATACOVES__DBT_HOME_REL_PATH"] = str(dbt_home_relpath)

        try:
            if self.get_config_value("update"):
                copier.run_update(
                    dst_path=self.repo_path,
                    data=self.copier_context,
                    quiet=self.get_config_value("quiet"),
                    unsafe=True,
                    overwrite=True,
                )
            else:
                copier.run_copy(
                    src_path=self.get_config_value("template_url"),
                    dst_path=self.repo_path,
                    data=self.copier_context,
                    quiet=self.get_config_value("quiet"),
                    unsafe=True,
                )
            return 0
        except TemplateSyntaxError as tserr:
            raise DbtCovesSetupException(
                f"Error in template file {tserr.filename}. Message: {tserr}"
            )
        except Exception as exc:
            raise exc
