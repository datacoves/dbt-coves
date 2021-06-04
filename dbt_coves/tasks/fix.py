
import sys
from subprocess import Popen, PIPE, CalledProcessError
from rich.console import Console

console = Console()


def execute(cmd):
    with Popen(cmd, stdout=PIPE, bufsize=1, universal_newlines=True) as p:
        for line in p.stdout:
            print(line, end='')  # process line here

    if p.returncode != 0:
        raise CalledProcessError(p.returncode, p.args)
    return p


def fix():
    command = execute(['sqlfluff', 'fix', '-f', '/config/workspace/models'])
    return command


class FixTask:
    def run(self) -> int:
        command = fix()

        sys.exit(command.returncode)
