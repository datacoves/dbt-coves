import os
from subprocess import CalledProcessError
import questionary

from pathlib import Path
from rich.console import Console

from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.shell import shell_run, run_and_capture
from .utils import print_row


console = Console()


class SetupSSHException(Exception):
    pass


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
        ssh_configured = False
        ssh_keys_dir = "~/.ssh/"
        ssh_keys_dir_abs = os.path.abspath(Path(ssh_keys_dir).expanduser())
        key_path_abs = f"{ssh_keys_dir_abs}/id_ecdsa"
        Path(ssh_keys_dir_abs).mkdir(parents=True, exist_ok=True)

        public_key_path_abs = f"{key_path_abs}.pub"

        found_keys = [
            file
            for file in os.listdir(ssh_keys_dir_abs)
            if "id_" in file.lower() and not ".pub" in file.lower()
        ]

        if found_keys:
            ssh_status = "[green]FOUND :heavy_check_mark:[/green]"
            print_row(
                f"Checking for SSH keys in '{ssh_keys_dir}'",
                ssh_status,
                new_section=False,
            )
            if len(found_keys) == 1:
                selected_ssh_key = found_keys[0]
            else:
                selected_ssh_key = questionary.select(
                    "Which of these SSH Keys would you like to associate to your dbt-coves project?:",
                    choices=found_keys,
                ).ask()

            key_path_abs = f"{ssh_keys_dir_abs}/{selected_ssh_key}"
            ssh_configured = cls.output_public_key_for_private(
                key_path_abs, public_key_path_abs
            )
        else:
            print_row(
                f"Checking for key in '{ssh_keys_dir}'", ssh_status, new_section=False
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
                ssh_key += "\n"
                with open(key_path_abs, "w") as file:
                    file.write(ssh_key)

                os.chmod(key_path_abs, 0o600)
                console.print(
                    f"[green]:heavy_check_mark: New SSH key stored on '{key_path_abs}'[/green]"
                )
                ssh_configured = cls.output_public_key_for_private(
                    key_path_abs, public_key_path_abs
                )
            if action == "generate":
                output = cls.generate_ecdsa_keys(key_path_abs)
                if output.returncode == 0:
                    console.print(
                        f"[green]:heavy_check_mark: SSH ecdsa key generated on '{key_path_abs}'[/green]"
                    )
                    ssh_configured = cls.output_public_key(public_key_path_abs)
        if ssh_configured:
            return 0
        else:
            raise Exception(
                f"You must first configure you SSH key in your Git server then rerun 'dbt-coves setup'"
            )

    @classmethod
    def generate_ecdsa_keys(cls, key_path_abs):
        try:
            return shell_run(
                args=["ssh-keygen", "-q", "-t", "ecdsa", "-f", key_path_abs]
            )
        except CalledProcessError as e:
            raise SetupSSHException(e.output)

    @classmethod
    def generate_ecdsa_public_key(cls, private_path_abs):
        keygen_args = [
            "ssh-keygen",
            "-y",
            "-f",
            private_path_abs,
            ">>",
            f"{private_path_abs}.pub",
        ]
        try:
            return shell_run(args=keygen_args)
        except CalledProcessError as e:
            raise SetupSSHException(e.output)

    @classmethod
    def output_public_key_for_private(cls, private_path_abs, public_key_path_abs):
        keygen_args = ["ssh-keygen", "-y", "-f", private_path_abs]
        public_output = run_and_capture(keygen_args)

        if public_output.stderr:
            raise SetupSSHException(public_output.stderr)

        if public_output.stdout:
            public_ssh_key = public_output.stdout
            with open(public_key_path_abs, "w") as file:
                file.write(public_ssh_key)
            return cls.output_public_key(public_key_path_abs)

    @classmethod
    def output_public_key(cls, public_key_path_abs):
        console.print(
            f"Please configure the following public key in your Git server (Gitlab, Github, Bitbucket, etc):\n"
        )
        print(open(public_key_path_abs, "r").read())
        return questionary.confirm(
            "Have you configured your Git provider with the key above?"
        ).ask()
