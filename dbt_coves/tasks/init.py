
import sys
from rich.console import Console
from dbt_coves.utils.shell import execute

console = Console()


def fix():
    return execute(['sqlfluff', 'fix', '-f', '/config/workspace/models'])


class InitTask:
    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "init", parents=[base_subparser], help="Initializes a new dbt project using predefined conventions."
        )
        subparser.set_defaults(cls=cls)
        return subparser

    @classmethod
    def from_args(cls, main_parser):
        return cls()

    def run(self) -> int:
        task = fix()

        sys.exit(task.returncode)
