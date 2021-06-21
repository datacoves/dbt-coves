"""Flags module containing the MainParser "Factory"."""
import sys
import os
from argparse import ArgumentParser
from pathlib import Path
from typing import List, Optional


class MainParser:
    """Sets flags from defaults or by parsing CLI arguments.

    In order to not have to always parse args when testing etc. We set the defaults explicitly here.
    This is a bit strict and not very DRY but it saves from surprises which for now is good.
    """

    def __init__(self, cli_parser: ArgumentParser) -> None:
        """Constructor for MainParser.

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
        self.template = "dbt-cookiecutter"

    def parse_args(self, cli_args: List[str] = list()) -> None:
        self.args = self.cli_parser.parse_args(cli_args or sys.argv[1:])

        if hasattr(self.args, 'profiles_dir'):
            self.args.profiles_dir = os.path.expanduser(self.args.profiles_dir)

        if getattr(self.args, 'project_dir', None) is not None:
            expanded_user = os.path.expanduser(self.args.project_dir)
            self.args.project_dir = os.path.abspath(expanded_user)

        self.task = self.args.task
        self.task_cls = self.args.cls

        if self.task:
            # base flags that need to be set no matter what
            if self.args:
                self.log_level = self.args.log_level
                self.verbose = self.args.verbose
                self.profiles_dir = self.args.profiles_dir
                self.project_dir = self.args.project_dir
                if self.args.config_path:
                    self.config_path = Path(self.args.config_path).expanduser()

            # task specific args consumption
            if self.task == "init":
                self.template = self.args.template
