
import sys
from subprocess import Popen, PIPE, CalledProcessError
import questionary
from rich.console import Console
from dbt_coves.tasks.fix import fix

console = Console()


def execute(cmd):
    with Popen(cmd, stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        for line in p.stdout:
            print(line, end='')  # process line here

    if p.returncode != 0:
        raise CalledProcessError(p.returncode, p.args)
    return p


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
