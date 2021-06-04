
import sys
import questionary
from rich.console import Console
from dbt_coves.tasks.fix import fix
from dbt_coves.utils.shell import execute

console = Console()


class CheckTask:
    def run(self) -> int:
        execute(['pre-commit', 'run', '--all-files'])

        command = execute(['sqlfluff', 'lint', '/config/workspace/models'])

        if command.returncode != 0:
            confirmed = questionary.confirm(
                "Would you like to auto-fix the issues?",
                default=True).ask()
            if confirmed:
                command = fix()
                sys.exit(command.returncode)

        return 0
