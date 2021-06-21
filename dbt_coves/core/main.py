import argparse
import sys
from typing import List

import pyfiglet
from dbt.config import PROFILES_DIR
from rich.console import Console

from dbt_coves.ui.traceback import DbtCovesTraceback
from dbt_coves.utils.flags import MainParser
from dbt_coves.config.config import DbtCovesConfig
from dbt_coves import __version__
from dbt_coves.utils.log import LOGGER as logger, log_manager
from dbt_coves.tasks.init import InitTask
from dbt_coves.tasks.generate import GenerateTask
from dbt_coves.tasks.check import CheckTask
from dbt_coves.tasks.fix import FixTask
from dbt_coves.core.exceptions import MissingCommand
from dbt_coves.tasks.deps import DepsTask

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
    '--project-dir',
    default=None,
    type=str,
    help='''
    Which directory to look in for the dbt_project.yml file.
    Default is the current working directory and its parents.
    '''
)

base_subparser.add_argument(
    '--profiles-dir',
    default=PROFILES_DIR,
    type=str,
    help='''
    Which directory to look in for the profiles.yml file. Default = {}
    '''.format(PROFILES_DIR)
)

base_subparser.add_argument(
    '--profile',
    required=False,
    type=str,
    help='''
    Which profile to load. Overrides setting in dbt_project.yml.
    '''
)

base_subparser.add_argument(
    '-t',
    '--target',
    default=None,
    type=str,
    help='''
    Which target to load for the given profile
    ''',
)

base_subparser.add_argument(
    '--vars',
    type=str,
    default='{}',
    help='''
    Supply variables to the project. This argument overrides variables
    defined in your dbt_project.yml file. This argument should be a YAML
    string, eg. '{my_variable: my_value}'
    '''
)


sub_parsers = parser.add_subparsers(title="dbt-coves commands", dest="task")

tasks = [InitTask, GenerateTask, CheckTask, FixTask, DepsTask]

[task.register_parser(sub_parsers, base_subparser) for task in tasks]


def handle(parser: argparse.ArgumentParser, cli_args: List[str] = list()) -> int:
    
    main_parser = MainParser(parser)
    main_parser.parse_args(cli_args=cli_args)

    if not main_parser.task:
        raise MissingCommand()
    
    # set up traceback manager fo prettier errors
    DbtCovesTraceback(main_parser)

    config = DbtCovesConfig(main_parser)
    config.load_config()

    # dbt_project = DbtProject(
    #     config.dbt_project_info.get("name", str()),
    #     config.dbt_project_info.get("path", str()),
    # )
    # dbt_project.read_project()

    # dbt_profile = DbtProfile(
    #     flags=main_parser,
    #     profile_name=dbt_project.profile_name,
    #     target_name=main_parser.target,
    #     profiles_dir=main_parser.profiles_dir,
    # )
    # dbt_profile.read_profile()

    if main_parser.log_level == "debug":
        log_manager.set_debug()

    task_cls = main_parser.task_cls
    if task_cls:
        task = task_cls.from_args(main_parser)
        return task.run()

    raise NotImplementedError(f"{main_parser.task} is not supported.")


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
        return 1

    if exit_code > 0:
        logger.error("[red]The process did not complete successfully.")
    return exit_code


if __name__ == "__main__":
    exit(main())
