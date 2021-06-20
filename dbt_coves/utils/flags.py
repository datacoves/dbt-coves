"""Flags module containing the FlagParser "Factory"."""
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import List, Optional


class FlagParser:
    """Sets flags from defaults or by parsing CLI arguments.

    In order to not have to always parse args when testing etc. We set the defaults explicitly here.
    This is a bit strict and not very DRY but it saves from surprises which for now is good.
    """

    def __init__(self, cli_parser: ArgumentParser) -> None:
        """Constructor for FlagParser.

        Holds explicit defaults and consumes parsed flags if asked for it.

        Args:
            cli_parser (ArgumentParser): CLI parser.
        """
        self.cli_parser = cli_parser
        self.log_level: str = "info"
        self.config_path: Path = Path(str())
        self.profiles_dir: Optional[Path] = None
        self.verbose: bool = False
        self.template = "dbt-cookiecutter"

    def set_args(self, cli_args: List[str] = list()) -> None:
        self.args = self.cli_parser.parse_args(cli_args or sys.argv[1:])

        self.task = self.args.command

        if self.task:
            # base flags that need to be set no matter what
            if self.args:
                self.log_level = self.args.log_level
                self.verbose = self.args.verbose
                if self.args.profiles_dir:
                    self.profiles_dir = Path(self.args.profiles_dir).expanduser()
                if self.args.config_path:
                    self.config_path = Path(self.args.config_path).expanduser()

            # task specific args consumption
            if self.task == "init":
                self.template = self.args.template
