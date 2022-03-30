import os
import questionary

from pathlib import Path
from rich.console import Console

from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.shell import shell_run
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
            "ssh",
            parents=[base_subparser],
            help="Set up SSH Key for dbt-coves project",
        )
        subparser.set_defaults(cls=cls, which="ssh")
        return subparser

    @classmethod
    def run(cls) -> int:
        ssh_status = "[red]MISSING[/red]"
        key_path = "~/.ssh/id_ecdsa"
        public_key_path = "~/.ssh/id_ecdsa.pub"
        key_path_abs = Path(key_path).expanduser()
        public_key_path_abs = Path(public_key_path).expanduser()
        ssh_exists = key_path_abs.exists()

        if ssh_exists:
            ssh_status = "[green]FOUND :heavy_check_mark:[/green]"
            print_row(
                f"Checking for key in '{key_path}'", ssh_status, new_section=False
            )
            cls.output_public_key(public_key_path_abs)

        if not ssh_exists:
            print_row(
                f"Checking for key in '{key_path}'", ssh_status, new_section=False
            )
            action = (
                questionary.select(
                    "Would you like to provide your existent private SSH key or generate a new one?",
                    choices=["Provide", "Generate"],
                )
                .ask()
                .lower()
            )
            if action == "provide":
                ssh_key = questionary.text("Please paste your private SSH key:").ask()
                key_path_abs.parent.mkdir(parents=True, exist_ok=True)
                with open(key_path_abs, "w") as file:
                    file.write(ssh_key)
                os.chmod(key_path_abs, 0o600)
                console.print(
                    f"[green]:heavy_check_mark: New SSH key stored on '{key_path}'[/green]"
                )
                cls.output_public_key(public_key_path_abs)
            if action == "generate":
                output = cls.generate_ecdsa_keys(key_path_abs)
                if output.returncode == 0:
                    console.print(
                        f"[green]:heavy_check_mark: SSH ecdsa key generated on '{key_path}'[/green]"
                    )
                    cls.output_public_key(public_key_path_abs)

        return 0

    @classmethod
    def generate_ecdsa_keys(cls, key_path_abs):
        return shell_run(args=["ssh-keygen", "-q", "-t", "ecdsa", "-f", key_path_abs])

    @classmethod
    def output_public_key(cls, public_key_path_abs):
        console.print(
            f"Please configure the following public key in your Git server (Gitlab, Github, Bitbucket, etc):"
        )
        print(open(public_key_path_abs, "r").read())
