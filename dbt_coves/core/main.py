import argparse
import sys
from typing import List

import pyfiglet
from rich.console import Console

from dbt_coves.dbt.profile import DbtProfile
from dbt_coves.dbt.project import DbtProject
from dbt_coves.ui.traceback import DbtCovesTraceback
from dbt_coves.utils.flags import FlagParser
from dbt_coves.config.config import DbtCovesConfig
from dbt_coves import __version__
from dbt_coves.utils.log import LOGGER as logger, log_manager
from dbt_coves.commands.init import InitCommand
from dbt_coves.commands.generate import GenerateCommand
from dbt_coves.commands.check import CheckCommand
from dbt_coves.commands.fix import FixCommand
from dbt_coves.core.exceptions import MissingCommand

console = Console()

parser = argparse.ArgumentParser(
    prog="dbt-coves",
    formatter_class=argparse.RawTextHelpFormatter,
    description="CLI tool for dbt users applying analytics engineering best practices.",
    epilog="Select one of the available sub-commands with --help to find out more about them.",
)

parser.add_argument(
    "-v",
    "--version",
    action="version",
    version=f"Installed dbt-coves version: {__version__}".rjust(40),
)

base_subparser = argparse.ArgumentParser(add_help=False)
base_subparser.add_argument(
    "--log-level", help="overrides default log level", type=str, default=str()
)
base_subparser.add_argument(
    "-vv",
    "--verbose",
    help="When provided the length of the tracebacks will not be truncated.",
    action="store_true",
    default=False,
)
base_subparser.add_argument(
    "--config-path", help="Full path to .dbt_coves file if not using default."
)
base_subparser.add_argument(
    "--profiles-dir", help="Alternative path to the dbt profiles.yml file.", type=str
)

sub_parsers = parser.add_subparsers(title="dbt-coves commands", dest="command")

COMMANDS = [InitCommand, GenerateCommand, CheckCommand, FixCommand]

[cmd.register_parser(sub_parsers, base_subparser) for cmd in COMMANDS]


def handle(parser: argparse.ArgumentParser, cli_args: List[str] = list()) -> int:
    
    flag_parser = FlagParser(parser)
    flag_parser.set_args(cli_args=cli_args)

    if not flag_parser.task:
        raise MissingCommand()
    
    # set up traceback manager fo prettier errors
    DbtCovesTraceback(flag_parser)

    config = DbtCovesConfig(flag_parser)
    config.load_config()

    dbt_project = DbtProject(
        config.dbt_project_info.get("name", str()),
        config.dbt_project_info.get("path", str()),
    )
    dbt_project.read_project()

    dbt_profile = DbtProfile(
        flags=flag_parser,
        profile_name=dbt_project.profile_name,
        target_name=flag_parser.target,
        profiles_dir=flag_parser.profiles_dir,
    )
    dbt_profile.read_profile()

    if flag_parser.log_level == "debug":
        log_manager.set_debug()

    task = None
    if flag_parser.task == "init":
        task = InitCommand()
    elif flag_parser.task == "generate":
        task = GenerateCommand()
    elif flag_parser.task == "check":
        task = CheckCommand()
    elif flag_parser.task == "fix":
        task = FixCommand()
    if task:
        return task.run()

    raise NotImplementedError(f"{flag_parser.task} is not supported.")


def main(parser: argparse.ArgumentParser = parser, test_cli_args: List[str] = list()) -> int:
    exit_code = 0
    cli_args = test_cli_args or []

    # print version on every run unless doing `--version` which is better handled by argparse
    if "--version" not in sys.argv[1:]:
        # app logo
        logo_str = str(pyfiglet.figlet_format("dbt-coves", font="standard"))
        console.print(logo_str, style="cyan")
        console.print("version 0.0.1\n")

    try:
        exit_code = handle(parser, cli_args)  # type: ignore
    except MissingCommand:
        parser.print_help()

    if exit_code > 0:
        logger.error("[red]The process did not complete successfully.")
    return exit_code


if __name__ == "__main__":
    exit(main())
