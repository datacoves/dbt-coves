import os
from pathlib import Path

import questionary
from rich.console import Console

from dbt_coves.tasks.base import NonDbtBaseTask
from .utils import print_row


console = Console()


class SetupSSHTask(NonDbtBaseTask):
    """
    Task that runs ssh key generation, git repo clone and db connection setup
    """

    key_column_with = 50
    value_column_with = 30

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "sshkey",
            parents=[base_subparser],
            help="Set up SSH Key for dbt-coves project",
        )
        subparser.set_defaults(cls=cls, which="sshkey")
        return subparser

    @classmethod
    def run(cls) -> int:
        ssh_status = "[red]MISSING[/red]"
        key_path = "~/.ssh/id_rsa"
        key_path_abs = Path(key_path).expanduser()
        ssh_exists = key_path_abs.exists()
        if ssh_exists:
            ssh_status = "[green]FOUND :heavy_check_mark:[/green]"
        print_row(f"Checking for key in '{key_path}'", ssh_status, new_section=True)
        confirmed = questionary.confirm(
            "Would you like to create a new SSH key?"
            if not ssh_exists
            else "Woud you like to overwrite your existing SSH key?",
            default=not ssh_exists,
        ).ask()
        if confirmed:
            ssh_key = questionary.text("Please paste your SSH key:").ask()
            key_path_abs.parent.mkdir(parents=True, exist_ok=True)
            with open(key_path_abs, "w") as file:
                file.write(ssh_key)
            os.chmod(key_path_abs, 0o600)
            console.print(
                f"[green]:heavy_check_mark: New SSH key stored on '{key_path}'[/green]"
            )
            return 0
