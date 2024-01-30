import argparse
import pathlib
import sys
import uuid
from subprocess import CalledProcessError
from typing import List

import pyfiglet
from dbt import tracking, version
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
from dbt_coves.utils.yaml import open_yaml, save_yaml

try:
    from dbt.flags import PROFILES_DIR

    VARS_DEFAULT_IS_STR = False
except ImportError:
    from dbt.cli.resolvers import default_profiles_dir

    PROFILES_DIR = default_profiles_dir()
    VARS_DEFAULT_IS_STR = True

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
    help="""Full path to .dbt_coves.yml file if not using default.
    Default is current working directory.""",
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
    dest="PROFILES_DIR",
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

if VARS_DEFAULT_IS_STR:
    base_subparser.add_argument(
        "--vars",
        type=str,
        default={},
        help="Supply variables to your dbt_project.yml file. This argument should be a YAML"
        " string, eg. '{my_variable: my_value}'",
    )
else:
    base_subparser.add_argument(
        "--vars",
        type=str,
        default="{}",
        help="Supply variables to your dbt_project.yml file. This argument should be a YAML"
        " string, eg. '{my_variable: my_value}'",
    )

base_subparser.add_argument(
    "--threads",
    type=str,
    default=None,
    help="Specify number of threads to use while executing models. Overrides settings in profiles.yml.",
)

base_subparser.add_argument(
    "--macro-debugging", action="store_true", default=False, dest="MACRO_DEBUGGING"
)

base_subparser.add_argument(
    "--version-check",
    action="store_true",
    default=False,
    help="If set, ensure the installed dbt version matches the require-dbt-version specified in the "
    "dbt_project.yml file (if any). Otherwise, allow them to differ.",
    dest="VERSION_CHECK",
)

base_subparser.add_argument(
    "--target-path",
    type=str,
    default=None,
    help="Configure the 'target-path'. Only applies this setting for the current run. "
    "Overrides the 'DBT_TARGET_PATH' if it is set.",
    dest="TARGET_PATH",
)

base_subparser.add_argument(
    "--log-path",
    type=str,
    default=None,
    help="Configure the 'log-path'. Only applies this setting for the current run. "
    "Overrides the 'DBT_LOG_PATH' if it is set.",
    dest="LOG_PATH",
)

base_subparser.add_argument(
    "--log-cache-events",
    action="store_true",
    default=False,
    help="Enable verbose logging for relational cache events to help when debugging.",
    dest="LOG_CACHE_EVENTS",
)

base_subparser.add_argument(
    "--send-anonymous-usage-stats",
    action="store_true",
    default=False,
    help="Whether dbt is configured to send anonymous usage statistics",
    dest="SEND_ANONYMOUS_USAGE_STATS",
)

base_subparser.add_argument(
    "--partial-parse",
    action="store_true",
    default=False,
    help="Allow for partial parsing by looking for and writing to a pickle file in the target directory. "
    "This overrides the user configuration file.",
    dest="PARTIAL_PARSE",
)

base_subparser.add_argument(
    "--static-parser",
    action="store_true",
    default=False,
    help="Use the static parser.",
    dest="STATIC_PARSER",
)

base_subparser.add_argument(
    "--disable-tracking",
    action="store_true",
    default=False,
    help="Disable command usage tracking. We don't store any user information.",
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
    _gen_get_app_uuid(main_parser.args)
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
    except CalledProcessError as cpe:
        import traceback

        logger.debug(traceback.format_exc())
        if cpe.returncode in [137, 247]:
            console.print(
                "[red]The process was killed by the OS due to running out of memory.[/red]"
            )
        console.print(f"[red]:cross_mark:[/red] {cpe.stderr}")

        return cpe.returncode
    except Exception as ex:
        import traceback

        logger.debug(traceback.format_exc())
        console.print(f"[red]:cross_mark:[/red] {ex}")
        return 1

    if exit_code > 0:
        logger.error("[red]The process did not complete successfully.")
    return exit_code


def _gen_get_app_uuid(args):
    dbt_coves_homepath = pathlib.Path("~/.dbt-coves/").expanduser()
    dbt_coves_homepath.mkdir(exist_ok=True)
    uuid_path = dbt_coves_homepath / ".user.yml"
    try:
        existent_uuid = open_yaml(uuid_path).get("id")
        args.uuid = existent_uuid
    except FileNotFoundError:
        dbt_coves_uuid = str(uuid.uuid4())
        dbt_coves_user = {"id": dbt_coves_uuid}
        save_yaml(uuid_path, dbt_coves_user)
        args.uuid = dbt_coves_uuid


if __name__ == "__main__":
    exit(main())
