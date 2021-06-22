import sys

from dbt_coves.utils.shell import execute

from .base import BaseTask


def fix():
    return execute(["sqlfluff", "fix", "-f", "/config/workspace/models"])


class FixTask(BaseTask):
    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "fix", parents=[base_subparser], help="Runs linter fixes."
        )
        subparser.set_defaults(cls=cls)
        return subparser

    def run(self) -> int:
        task = fix()

        sys.exit(task.returncode)
