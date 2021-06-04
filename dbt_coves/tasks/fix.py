
import sys
import subprocess
from rich.console import Console

console = Console()


def fix():
    command = subprocess.run(
        ['sqlfluff', 'fix', '/config/workspace/models'], capture_output=True)

    sys.stdout.buffer.write(command.stdout)
    sys.stderr.buffer.write(command.stderr)
    return command


class FixTask:
    def run(self) -> int:
        command = fix()

        sys.exit(command.returncode)
