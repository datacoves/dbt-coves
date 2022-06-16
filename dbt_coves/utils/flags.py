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
        self.profiles_dir: Optional[Path] = None
        self.project_dir: Optional[Path] = None
        self.verbose: bool = False
        self.generate = {
            "sources": {
                "relations": [],
                "database": None,
                "schemas": [],
                "destination": None,
                "model-props-strategy": None,
                "templates-folder": None,
                "metadata": None,
            }
        }
        self.extract = {
            "airbyte": {"path": None, "host": None, "port": None, "dbt-list-args": None}
        }
        self.load = {
            "airbyte": {
                "path": None,
                "host": None,
                "port": None,
                "secrets-manager": None,
                "secrets-url": None,
                "secrets-token": None,
                "secrets-path": None,
                "dbt-list-args": None,
            }
        }
        self.init = {
            "template": "https://github.com/datacoves/cookiecutter-dbt.git",
            "current-dir": False,
        }
        self.check = {"no-fix": False, "slim": False}
        self.setup = {
            "all": {"templates": None, "open-ssl-public-key": False},
            "sqlfluff": {"templates": None},
            "precommit": {"templates": None},
            "ssh": {"open-ssl-public-key": False},
        }

    def parse_args(self, cli_args: List[str] = list()) -> None:
        self.args = self.cli_parser.parse_args(cli_args or sys.argv[1:])

        if hasattr(self.args, "profiles_dir"):
            self.args.profiles_dir = os.path.expanduser(self.args.profiles_dir)

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
                if self.args.profiles_dir:
                    self.profiles_dir = self.args.profiles_dir
                if self.args.project_dir:
                    self.project_dir = self.args.project_dir
                if self.args.config_path:
                    self.config_path = Path(self.args.config_path).expanduser()

            # generate sources
            if self.args.cls.__name__ == "GenerateSourcesTask":
                if self.args.schemas:
                    self.generate["sources"]["schemas"] = [
                        schema.strip() for schema in self.args.schemas.split(",")
                    ]
                if self.args.database:
                    self.generate["sources"]["database"] = self.args.database
                if self.args.relations:
                    self.generate["sources"]["relations"] = [
                        relation.strip() for relation in self.args.relations.split(",")
                    ]
                if self.args.destination:
                    self.generate["sources"]["destination"] = self.args.destination
                if self.args.model_props_strategy:
                    self.generate["sources"][
                        "model-props-strategy"
                    ] = self.args.model_props_strategy
                if self.args.templates_folder:
                    self.generate["sources"][
                        "templates-folder"
                    ] = self.args.templates_folder
                if self.args.metadata:
                    self.generate["sources"]["metadata"] = self.args.metadata

            if self.args.cls.__name__ == "InitTask":
                if self.args.template:
                    self.init["template"] = self.args.template
                if self.args.current_dir:
                    self.init["current-dir"] = self.args.current_dir

            if self.args.cls.__name__ == "CheckTask":
                if self.args.no_fix:
                    self.check["no-fix"] = self.args.no_fix
                if self.args.slim:
                    self.check["slim"] = self.args.slim

            if self.args.cls.__name__ == "LoadAirbyteTask":
                if self.args.path:
                    self.load["airbyte"]["path"] = self.args.path

                if self.args.host and self.args.port:
                    self.load["airbyte"]["port"] = self.args.port
                    self.load["airbyte"]["host"] = self.args.host
                if self.args.secrets_path:
                    self.load["airbyte"]["secrets-path"] = self.args.secrets_path
                if self.args.secrets_url:
                    self.load["airbyte"]["secrets-url"] = self.args.secrets_url
                if self.args.secrets_token:
                    self.load["airbyte"]["secrets-token"] = self.args.secrets_token
                if self.args.secrets_path:
                    self.load["airbyte"]["secrets-path"] = self.args.secrets_path
                if self.args.secrets_path:
                    self.load["airbyte"]["dbt-list-args"] = self.args.dbt_list_args

            if self.args.cls.__name__ == "ExtractAirbyteTask":
                if self.args.path:
                    self.extract["airbyte"]["path"] = self.args.path
                if self.args.host and self.args.port:
                    self.extract["airbyte"]["host"] = self.args.host
                    self.extract["airbyte"]["port"] = self.args.port
                if self.args.dbt_list_args:
                    self.extract["airbyte"]["dbt-list-args"] = self.args.dbt_list_args

            if self.args.cls.__name__ == "SetupAllTask":
                if self.args.templates:
                    self.setup["all"]["templates"] = self.args.templates
                if self.args.open_ssl_public_key:
                    self.setup["all"][
                        "open-ssl-public-key"
                    ] = self.args.open_ssl_public_key

            if self.args.cls.__name__ == "SetupSqlfluffTask":
                if self.args.templates:
                    self.setup["sqlfluff"]["templates"] = self.args.templates

            if self.args.cls.__name__ == "SetupPrecommitTask":
                if self.args.templates:
                    self.setup["precommit"]["templates"] = self.args.templates

            if self.args.cls.__name__ == "SetupSSHTask":
                if self.args.open_ssl_public_key:
                    self.setup["ssh"][
                        "open-ssl-public-key"
                    ] = self.args.open_ssl_public_key
