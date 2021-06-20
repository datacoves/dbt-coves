
import sys
from dbt_coves.utils.shell import execute


def fix():
    return execute(['sqlfluff', 'fix', '-f', '/config/workspace/models'])


class FixCommand:
    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "fix", parents=[base_subparser], help="Runs linter fixes."
        )
        return subparser

    def run(self) -> int:
        command = fix()

        sys.exit(command.returncode)
