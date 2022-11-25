import argparse
import sys
from typing import List

import pyfiglet
from dbt import tracking, version
from dbt.flags import PROFILES_DIR
from rich.console import Console

from dbt_coves import __version__
from dbt_coves.config.config import DbtCovesConfig
from dbt_coves.core.exceptions import MissingCommand, MissingDbtProject
from dbt_coves.tasks.base import BaseTask
from dbt_coves.tasks.dbt.main import RunDbtTask
from dbt_coves.tasks.extract.main import ExtractTask
from dbt_coves.tasks.generate.main import GenerateTask
from dbt_coves.tasks.load.main import LoadTask
from dbt_coves.tasks.setup.main import SetupTask
from dbt_coves.ui.traceback import DbtCovesTraceback
from dbt_coves.utils.flags import DbtCovesFlags
from dbt_coves.utils.log import LOGGER as logger
from dbt_coves.utils.log import log_manager

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
    "--config-path",
    help="Full path to .dbt_coves.yml file if not using default. Default is current working directory.",
)

base_subparser.add_argument(
    "--project-dir",
    default=None,
    type=str,
    help="Which directory to look in for the dbt_project.yml file."
    " Default is the current working directory and its parents.",
)

base_subparser.add_argument(
    "--profiles-dir",
    default=PROFILES_DIR,
    type=str,
    help="Which directory to look in for the profiles.yml file.",
)

base_subparser.add_argument(
    "--profile",
    required=False,
    type=str,
    help="Which profile to load. Overrides setting in dbt_project.yml.",
)

base_subparser.add_argument(
    "-t",
    "--target",
    default=None,
    type=str,
    help="Which target to load for the given profile",
)

base_subparser.add_argument(
    "--vars",
    type=str,
    default="{}",
    help="Supply variables to your dbt_project.yml file. This argument should be a YAML"
    " string, eg. '{my_variable: my_value}'",
)


sub_parsers = parser.add_subparsers(title="dbt-coves commands", dest="task")

# Register subcommands
[
    task.register_parser(sub_parsers, base_subparser)
    for task in [GenerateTask, SetupTask, ExtractTask, LoadTask, RunDbtTask]
]


def handle(parser: argparse.ArgumentParser, cli_args: List[str] = list()) -> int:
    main_parser = DbtCovesFlags(parser)
    main_parser.parse_args(cli_args=cli_args)

    if not main_parser.task_cls:
        raise MissingCommand(main_parser.cli_parser)
    else:
        task_cls: BaseTask = main_parser.task_cls

    # set up traceback manager fo prettier errors
    DbtCovesTraceback(main_parser)

    coves_config = None
    if task_cls.needs_config:
        coves_config = DbtCovesConfig(main_parser)
        coves_config.load_config()

    if main_parser.log_level == "debug":
        log_manager.set_debug()

    return task_cls.get_instance(main_parser, coves_config=coves_config).run()


def main(parser: argparse.ArgumentParser = parser, test_cli_args: List[str] = list()) -> int:
    tracking.do_not_track()

    exit_code = 0
    cli_args = test_cli_args or []

    # print version on every run unless doing `--version` which is better handled by argparse
    if "--version" not in sys.argv[1:]:
        # app logo
        logo_str = str(pyfiglet.figlet_format("dbt-coves", font="standard"))
        console.print(logo_str, style="cyan")
        dbt_version = version.get_installed_version().to_version_string(skip_matcher=True)
        console.print(f"dbt-coves v{__version__}".ljust(24) + f"dbt v{dbt_version}\n".rjust(23))

    try:
        exit_code = handle(parser, cli_args)  # type: ignore
    except MissingCommand as e:
        e.print_help()
        return 1
    except MissingDbtProject:
        console.print(
            "No [u]dbt_project.yml[/u] found. Current folder doesn't look like a dbt project."
        )
        return 1
    except Exception as ex:
        import traceback

        logger.debug(traceback.format_exc())
        console.print(f"[red]:cross_mark:[/red] {ex}")
        return 1

    if exit_code > 0:
        logger.error("[red]The process did not complete successfully.")
    return exit_code


if __name__ == "__main__":
    exit(main())
