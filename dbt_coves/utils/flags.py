"""Flags module containing the DbtCovesFlags "Factory"."""
import os
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import List, Optional


class DbtCovesFlags:
    """Sets flags from defaults or by parsing CLI arguments.

    In order to not have to always parse args when testing etc. We set the defaults explicitly here.
    This is a bit strict and not very DRY but it saves from surprises which for now is good.
    """

    def __init__(self, cli_parser: ArgumentParser) -> None:
        """Constructor for DbtCovesFlags.

        Holds explicit defaults and consumes parsed flags if asked for it.

        Args:
            cli_parser (ArgumentParser): CLI parser.
        """
        self.cli_parser = cli_parser
        self.log_level: str = "info"
        self.config_path: Path = Path(str())
        self.PROFILES_DIR: Optional[Path] = None
        self.project_dir: Optional[Path] = None
        self.threads: str = None
        self.MACRO_DEBUGGING: bool = False
        self.VERSION_CHECK: bool = False
        self.TARGET_PATH: str = None
        self.LOG_PATH: str = None
        self.LOG_CACHE_EVENTS: bool = False
        self.verbose: bool = False
        self.generate = {
            "sources": {
                "select_relations": [],
                "exclude_relations": [],
                "database": None,
                "schemas": [],
                "sources_destination": None,
                "models_destination": None,
                "model_props_destination": None,
                "update_strategy": None,
                "templates_folder": None,
                "metadata": None,
                "no_prompt": False,
                "flatten_json_fields": None,
                "overwrite_staging_models": False,
                "skip_model_props": False,
            },
            "properties": {
                "templates_folder": None,
                "metadata": None,
                "destination": None,
                "update_strategy": None,
                "select": None,
                "exclude": None,
                "selector": None,
                "no_prompt": False,
            },
            "metadata": {
                "database": None,
                "schemas": [],
                "select_relations": [],
                "exclude_relations": [],
                "destination": None,
                "no_prompt": False,
            },
            "docs": {
                "merge_deferred": False,
                "state": None,
            },
            "airflow_dags": {
                "yml_path": None,
                "dags_path": None,
                "validate_operators": False,
                "generators_folder": None,
                "generators_params": None,
                "secrets_path": None,
                "secrets_manager": None,
                "secrets_url": None,
                "secrets_token": None,
                "secrets_project": None,
                "secrets_tags": None,
                "secrets_key": None,
            },
        }
        self.extract = {
            "airbyte": {
                "path": None,
                "host": None,
                "port": None,
            },
            "fivetran": {
                "path": None,
                "api_key": None,
                "api_secret": None,
                "credentials": None,
            },
        }
        self.load = {
            "airbyte": {
                "path": None,
                "host": None,
                "port": None,
                "secrets_path": None,
                "secrets_manager": None,
                "secrets_url": None,
                "secrets_token": None,
                "secrets_project": None,
                "secrets_tags": None,
                "secrets_key": None,
            },
            "fivetran": {
                "path": None,
                "api_key": None,
                "api_secret": None,
                "secrets_path": None,
                "credentials": None,
                "secrets_manager": None,
                "secrets_url": None,
                "secrets_token": None,
                "secrets_project": None,
                "secrets_tags": None,
                "secrets_key": None,
            },
        }
        self.init = {
            "template": "https://github.com/datacoves/cookiecutter-dbt.git",
            "current-dir": False,
        }
        self.setup = {
            "ssh": {"open_ssl_public_key": False},
            "git": {"no_prompt": False},
        }
        self.dbt = {"command": None, "project_dir": None, "virtualenv": None, "cleanup": False}

    def parse_args(self, cli_args: List[str] = list()) -> None:
        self.args = self.cli_parser.parse_args(cli_args or sys.argv[1:])

        if hasattr(self.args, "PROFILES_DIR"):
            self.args.PROFILES_DIR = os.path.expanduser(self.args.PROFILES_DIR)

        if getattr(self.args, "project_dir", None) is not None:
            expanded_user = os.path.expanduser(self.args.project_dir)
            self.args.project_dir = os.path.abspath(expanded_user)
        self.task = self.args.task
        self.task_cls = getattr(self.args, "cls", None)

        if self.task:
            if self.args:
                if self.args.log_level:
                    self.log_level = self.args.log_level
                if self.args.verbose:
                    self.verbose = self.args.verbose
                if self.args.PROFILES_DIR:
                    self.PROFILES_DIR = self.args.PROFILES_DIR
                if self.args.project_dir:
                    self.project_dir = self.args.project_dir
                if self.args.config_path:
                    self.config_path = Path(self.args.config_path).expanduser()
                if self.args.threads:
                    self.threads = self.args.threads
                if self.args.MACRO_DEBUGGING:
                    self.MACRO_DEBUGGING = self.args.MACRO_DEBUGGING
                if self.args.VERSION_CHECK:
                    self.VERSION_CHECK = self.args.VERSION_CHECK
                if self.args.TARGET_PATH:
                    self.TARGET_PATH = self.args.TARGET_PATH
                if self.args.LOG_PATH:
                    self.LOG_PATH = self.args.LOG_PATH
                if self.args.LOG_CACHE_EVENTS:
                    self.LOG_CACHE_EVENTS = self.args.LOG_CACHE_EVENTS

            # generate sources
            if self.args.cls.__name__ == "GenerateSourcesTask":
                if self.args.schemas:
                    self.generate["sources"]["schemas"] = [
                        schema.strip() for schema in self.args.schemas.split(",")
                    ]
                if self.args.database:
                    self.generate["sources"]["database"] = self.args.database
                if self.args.select_relations:
                    self.generate["sources"]["select_relations"] = [
                        relation.strip() for relation in self.args.select_relations.split(",")
                    ]
                if self.args.sources_destination:
                    self.generate["sources"]["sources_destination"] = self.args.sources_destination
                if self.args.models_destination:
                    self.generate["sources"]["models_destination"] = self.args.models_destination
                if self.args.model_props_destination:
                    self.generate["sources"][
                        "model_props_destination"
                    ] = self.args.model_props_destination
                if self.args.update_strategy:
                    self.generate["sources"]["update_strategy"] = self.args.update_strategy
                if self.args.templates_folder:
                    self.generate["sources"]["templates_folder"] = self.args.templates_folder
                if self.args.metadata:
                    self.generate["sources"]["metadata"] = self.args.metadata
                if self.args.exclude_relations:
                    self.generate["sources"][
                        "exclude_relations"
                    ] = self.args.exclude_relations.split(",")
                if self.args.no_prompt:
                    self.generate["sources"]["no_prompt"] = True
                if self.args.flatten_json_fields:
                    self.generate["sources"][
                        "flatten_json_fields"
                    ] = self.args.flatten_json_fields.lower()
                if self.args.overwrite_staging_models:
                    self.generate["sources"]["overwrite_staging_models"] = True
                if self.args.skip_model_props:
                    self.generate["sources"]["skip_model_props"] = True

            # generate properties
            if self.args.cls.__name__ == "GeneratePropertiesTask":
                if self.args.templates_folder:
                    self.generate["properties"]["templates_folder"] = self.args.templates_folder
                if self.args.metadata:
                    self.generate["properties"]["metadata"] = self.args.metadata
                if self.args.destination:
                    self.generate["properties"]["destination"] = self.args.destination
                if self.args.update_strategy:
                    self.generate["sources"]["update_strategy"] = self.args.update_strategy
                if self.args.select:
                    self.generate["properties"]["select"] = self.args.select
                if self.args.exclude:
                    self.generate["properties"]["exclude"] = self.args.exclude
                if self.args.selector:
                    self.generate["properties"]["selector"] = self.args.selector
                if self.args.no_prompt:
                    self.generate["properties"]["no_prompt"] = True

            # generate metadata
            if self.args.cls.__name__ == "GenerateMetadataTask":
                if self.args.database:
                    self.generate["metadata"]["database"] = self.args.database
                if self.args.schemas:
                    self.generate["metadata"]["schemas"] = [
                        schema.strip() for schema in self.args.schemas.split(",")
                    ]
                if self.args.select_relations:
                    self.generate["metadata"]["select_relations"] = [
                        relation.strip() for relation in self.args.select_relations.split(",")
                    ]
                if self.args.exclude_relations:
                    self.generate["metadata"]["exclude_relations"] = [
                        relation.strip() for relation in self.args.exclude_relations.split(",")
                    ]
                if self.args.destination:
                    self.generate["metadata"]["destination"] = self.args.destination
                if self.args.no_prompt:
                    self.generate["metadata"]["no_prompt"] = True

            # generate docs
            if self.args.cls.__name__ == "GenerateDocsTask":
                if self.args.merge_deferred:
                    self.generate["docs"]["merge_deferred"] = self.args.merge_deferred
                if self.args.state:
                    self.generate["docs"]["state"] = self.args.state

            # generate airflow_dags
            if self.args.cls.__name__ == "GenerateAirflowDagsTask":
                if self.args.yml_path:
                    self.generate["airflow_dags"]["yml_path"] = self.args.yml_path
                if self.args.dags_path:
                    self.generate["airflow_dags"]["dags_path"] = self.args.dags_path
                if self.args.validate_operators:
                    self.generate["airflow_dags"][
                        "validate_operators"
                    ] = self.args.validate_operators
                if self.args.generators_folder:
                    self.generate["airflow_dags"]["generators_folder"] = self.args.generators_folder
                if self.args.generators_params:
                    self.generate["airflow_dags"]["generators_params"] = self.args.generators_params
                if self.args.secrets_path:
                    self.generate["airflow_dags"]["secrets_path"] = self.args.secrets_path
                if self.args.secrets_manager:
                    self.generate["airflow_dags"]["secrets_manager"] = self.args.secrets_manager
                if self.args.secrets_url:
                    self.generate["airflow_dags"]["secrets_url"] = self.args.secrets_url
                if self.args.secrets_token:
                    self.generate["airflow_dags"]["secrets_token"] = self.args.secrets_token
                if self.args.secrets_project:
                    self.generate["airflow_dags"]["secrets_project"] = self.args.secrets_project
                if self.args.secrets_tags:
                    self.generate["airflow_dags"]["secrets_tags"] = self.args.secrets_tags
                if self.args.secrets_key:
                    self.generate["airflow_dags"]["secrets_key"] = self.args.secrets_key

            # load airbyte
            if self.args.cls.__name__ == "LoadAirbyteTask":
                if self.args.path:
                    self.load["airbyte"]["path"] = self.args.path

                if self.args.host and self.args.port:
                    self.load["airbyte"]["port"] = self.args.port
                    self.load["airbyte"]["host"] = self.args.host
                if self.args.secrets_path:
                    self.load["airbyte"]["secrets_path"] = self.args.secrets_path
                if self.args.secrets_manager:
                    self.load["airbyte"]["secrets_manager"] = self.args.secrets_manager
                if self.args.secrets_url:
                    self.load["airbyte"]["secrets_url"] = self.args.secrets_url
                if self.args.secrets_token:
                    self.load["airbyte"]["secrets_token"] = self.args.secrets_token
                if self.args.secrets_project:
                    self.load["airbyte"]["secrets_project"] = self.args.secrets_project
                if self.args.secrets_tags:
                    self.load["airbyte"]["secrets_tags"] = [
                        tag.strip() for tag in self.args.secrets_tags.split(",")
                    ]
                if self.args.secrets_key:
                    self.load["airbyte"]["secrets_key"] = self.args.secrets_key

            # load fivetran
            if self.args.cls.__name__ == "LoadFivetranTask":
                if self.args.path:
                    self.load["fivetran"]["path"] = self.args.path
                if self.args.api_key:
                    self.load["fivetran"]["api_key"] = self.args.api_key
                if self.args.api_secret:
                    self.load["fivetran"]["api_secret"] = self.args.api_secret
                if self.args.secrets_path:
                    self.load["fivetran"]["secrets_path"] = self.args.secrets_path
                if self.args.credentials:
                    self.load["fivetran"]["credentials"] = self.args.credentials
                if self.args.secrets_manager:
                    self.load["fivetran"]["secrets_manager"] = self.args.secrets_manager
                if self.args.secrets_url:
                    self.load["fivetran"]["secrets_url"] = self.args.secrets_url
                if self.args.secrets_token:
                    self.load["fivetran"]["secrets_token"] = self.args.secrets_token
                if self.args.secrets_project:
                    self.load["fivetran"]["secrets_project"] = self.args.secrets_project
                if self.args.secrets_tags:
                    self.load["fivetran"]["secrets_tags"] = [
                        tag.strip() for tag in self.args.secrets_tags.split(",")
                    ]
                if self.args.secrets_key:
                    self.load["fivetran"]["secrets_key"] = self.args.secrets_key

            # extract airbyte
            if self.args.cls.__name__ == "ExtractAirbyteTask":
                if self.args.path:
                    self.extract["airbyte"]["path"] = self.args.path
                if self.args.host and self.args.port:
                    self.extract["airbyte"]["host"] = self.args.host
                    self.extract["airbyte"]["port"] = self.args.port

            # extract fivetran
            if self.args.cls.__name__ == "ExtractFivetranTask":
                if self.args.path:
                    self.extract["fivetran"]["path"] = self.args.path
                if self.args.api_key:
                    self.extract["fivetran"]["api_key"] = self.args.api_key
                if self.args.api_secret:
                    self.extract["fivetran"]["api_secret"] = self.args.api_secret
                if self.args.credentials:
                    self.extract["fivetran"]["credentials"] = self.args.credentials

            # setup ssh
            if self.args.cls.__name__ == "SetupSSHTask":
                if self.args.open_ssl_public_key:
                    self.setup["ssh"]["open_ssl_public_key"] = self.args.open_ssl_public_key

            # setup git
            if self.args.cls.__name__ == "SetupGitTask":
                if self.args.no_prompt:
                    self.setup["git"]["no_prompt"] = self.args.no_prompt

            # run dbt
            if self.args.cls.__name__ == "RunDbtTask":
                if self.args.command:
                    self.dbt["command"] = self.args.command
                if self.args.project_dir:
                    self.dbt["project_dir"] = self.args.project_dir
                if self.args.virtualenv:
                    self.dbt["virtualenv"] = self.args.virtualenv
                if self.args.cleanup:
                    self.dbt["cleanup"] = self.args.cleanup
