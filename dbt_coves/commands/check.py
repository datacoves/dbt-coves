
import sys
import questionary
from dbt_coves.commands.fix import fix
from dbt_coves.utils.shell import execute


class CheckCommand:
    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "check", parents=[base_subparser], help="Runs pre-commit hooks and linters."
        )
        return subparser

    def run(self) -> int:
        execute(['pre-commit', 'run', '--all-files'])

        command = execute(['sqlfluff', 'lint', '/config/workspace/models'])

        if command.returncode != 0:
            confirmed = questionary.confirm(
                "Would you like to try auto-fix these issues?",
                default=True).ask()
            if confirmed:
                command = fix()
                sys.exit(command.returncode)

        return 0
