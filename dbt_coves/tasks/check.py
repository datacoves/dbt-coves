
import sys
import subprocess
import questionary
from rich.console import Console
from dbt_coves.tasks.fix import fix

console = Console()


class CheckTask:
    def run(self) -> int:
        command = subprocess.run(
            ['sqlfluff', 'lint', '/config/workspace/models'], capture_output=True)

        sys.stdout.buffer.write(command.stdout)
        sys.stderr.buffer.write(command.stderr)

        if command.returncode == 0:
            command = subprocess.run(
                ['pre-commit', 'run', '--all-files'], capture_output=True)

            sys.stdout.buffer.write(command.stdout)
            sys.stderr.buffer.write(command.stderr)

            sys.exit(command.returncode)
        else:
            confirmed = questionary.confirm(
                "Would you like to auto-fix the issues?",
                default=True).ask()
            if confirmed:
                command = fix()
                sys.exit(command.returncode)
            else:
                return 0
