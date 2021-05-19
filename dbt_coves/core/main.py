import argparse
import sys
from typing import List

import pyfiglet
from rich.console import Console

from dbt_coves import __version__
from dbt_coves.utils.log import LOGGER as logger
from dbt_coves.tasks.generate import GenerateTask

console = Console()

parser = argparse.ArgumentParser(
    prog="dbt-coves",
    formatter_class=argparse.RawTextHelpFormatter,
    description="CLI tool for dbt users that follow the datacoves guidelines.",
    epilog="Select onf of the available sub-commands with --help to find out more about them.",
)

parser.add_argument(
    "-v",
    "--version",
    action="version",
    version=f"Installed dbt-sugar version: {__version__}".rjust(40),
)

base_subparser = argparse.ArgumentParser(add_help=False)
base_subparser.add_argument(
    "--log-level", help="overrides default log level", type=str, default=str()
)

sub_parsers = parser.add_subparsers(title="dbt-coves commands", dest="command")

# generate task parser
generate_sub_parser = sub_parsers.add_parser(
    "generate", parents=[base_subparser], help="Generates models, docs and tests."
)

# generate_sub_parser.set_defaults(cls=GenerateTask, which="generate")

generate_sub_parser.add_argument(
    "-s",
    "--sources",
    help="Name of the sources to generate",
    type=str,
    default=None,
)


def handle(parser: argparse.ArgumentParser, test_cli_args: List[str] = list()) -> int:

    _cli_args = test_cli_args or sys.argv[1:]
    args = parser.parse_args(_cli_args)

    task_name = args.command

    if task_name == "generate":
        task: GenerateTask = GenerateTask()
        return task.run()

    raise NotImplementedError(f"{task_name} is not supported.")


def main(
    parser: argparse.ArgumentParser = parser, test_cli_args: List[str] = list()
) -> int:
    exit_code = 0
    _cli_args = []
    if test_cli_args:
        _cli_args = test_cli_args

    # print version on every run unless doing `--version` which is better handled by argparse
    if "--version" not in sys.argv[1:]:
        # app logo
        logo_str = str(pyfiglet.figlet_format("dbt-coves", font="slant"))
        console.print(logo_str, style="blue")

    exit_code = handle(parser, _cli_args)  # type: ignore

    if exit_code > 0:
        logger.error("[red]The process did not complete successfully.")
    return exit_code


if __name__ == "__main__":
    exit(main())
