
import sys
from subprocess import Popen, PIPE, CalledProcessError
from rich.console import Console
from dbt_coves.utils.shell import execute

console = Console()


def fix():
    return execute(['sqlfluff', 'fix', '-f', '/config/workspace/models'])


class FixTask:
    def run(self) -> int:
        command = fix()

        sys.exit(command.returncode)
