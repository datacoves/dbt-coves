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
                "model_props_strategy": None,
                "templates_folder": None,
                "metadata": None,
            }
        }
        self.extract = {
            "airbyte": {"path": None, "host": None, "port": None, "dbt_list_args": None}
        }
        self.load = {
            "airbyte": {"path": None, "host": None, "port": None, "secrets": None}
        }
        self.init = {
            "template": "https://github.com/datacoves/cookiecutter-dbt.git",
            "current-dir": False,
        }
        self.check = {"no-fix": False}

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
                        "model_props_strategy"
                    ] = self.args.model_props_strategy
                if self.args.templates_folder:
                    self.generate["sources"][
                        "templates_folder"
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

            if self.args.cls.__name__ == "LoadAirbyteTask":
                if self.args.path:
                    self.load["airbyte"]["path"] = self.args.path

                if self.args.host and self.args.port:
                    self.load["airbyte"]["port"] = self.args.port
                    self.load["airbyte"]["host"] = self.args.host
                if self.args.secrets:
                    self.load["airbyte"]["secrets"] = self.args.secrets

            if self.args.cls.__name__ == "ExtractAirbyteTask":
                if self.args.path:
                    self.extract["airbyte"]["path"] = self.args.path
                if self.args.host and self.args.port:
                    self.extract["airbyte"]["host"] = self.args.host
                    self.extract["airbyte"]["port"] = self.args.port
                if self.args.dbt_list_args:
                    self.extract["airbyte"]["dbt_list_args"] = self.args.dbt_list_args
