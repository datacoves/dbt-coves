import os
from subprocess import CalledProcessError
import questionary
import subprocess

from pathlib import Path
from rich.console import Console

from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.shell import shell_run, run_and_capture, run_and_capture_shell
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
        subparser.add_argument(
            "--open-ssl-public-key",
            help="Generate and output OpenSSL key alongside Git OpenSSH one",
            action="store_true",
            default=False,
        )
        subparser.set_defaults(cls=cls, which="ssh")
        cls.arg_parser = base_subparser
        return subparser

    def run(self) -> int:
        ssh_status = "[red]MISSING[/red]"
        ssh_configured = False
        ssh_keys_dir = "~/.ssh/"
        self.ssh_keys_dir_abs = os.path.abspath(Path(ssh_keys_dir).expanduser())

        provided_key_path = f"{self.ssh_keys_dir_abs}/id_datacoves"

        key_path_abs = f"{self.ssh_keys_dir_abs}/id_ecdsa"
        Path(self.ssh_keys_dir_abs).mkdir(parents=True, exist_ok=True)

        public_key_path_abs = f"{key_path_abs}.pub"

        found_keys = [
            file
            for file in os.listdir(self.ssh_keys_dir_abs)
            if "id_" in file.lower() and not ".p" in file.lower()
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

            key_path_abs = f"{self.ssh_keys_dir_abs}/{selected_ssh_key}"
            public_key_path_abs = f"{key_path_abs}.pub"

            ssh_configured = self.output_public_key_for_private(
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
                with open(provided_key_path, "w") as file:
                    file.write(ssh_key)

                os.chmod(provided_key_path, 0o600)

                ssh_configured = self.transform_default_private(provided_key_path)
            if action == "generate":
                output = self.generate_ecdsa_keys(key_path_abs)
                if output.returncode == 0:
                    console.print(
                        f"[green]:heavy_check_mark: SSH ecdsa key generated on '{key_path_abs}'[/green]"
                    )
                    ssh_configured = self.output_public_keys(public_key_path_abs)
        if ssh_configured:
            return 0
        else:
            raise Exception(
                f"You must first configure you SSH key in your Git server then rerun 'dbt-coves setup'"
            )

    def generate_ecdsa_keys(self, key_path_abs):
        try:
            return shell_run(
                args=["ssh-keygen", "-q", "-t", "ecdsa", "-f", key_path_abs]
            )
        except CalledProcessError as e:
            raise SetupSSHException(e.output)

    def generate_ecdsa_public_key(self, private_path_abs):
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

    def transform_default_private(self, provided_key_path):
        types_filename_dict = {
            "ssh-dss": "id_dsa",
            "ecdsa-sha2-nistp256": "id_ecdsa",
            "ssh-ed25519": "id_ed25519",
            "ssh-rsa": "id_rsa",
        }
        # Get public key from private
        public_output, public_type = self.ssh_keygen_get_public_key(provided_key_path)

        ssh_file_name = types_filename_dict.get(public_type)

        if not ssh_file_name:
            os.remove(provided_key_path)
            raise SetupSSHException(
                f"Provided ssh key type {public_type} is not supported (must provide dsa/ecdsa/ed25519/rsa). Please try again"
            )

        private_key_path = provided_key_path.replace("id_datacoves", ssh_file_name)

        os.rename(provided_key_path, private_key_path)
        public_key_path = f"{private_key_path}.pub"

        with open(public_key_path, "w") as file:
            file.write(public_output)

        openssl_private_path = private_key_path if public_type == "ssh-rsa" else None

        # Return public key to configure
        return self.output_public_keys(public_key_path, openssl_private_path)

    def gen_openssl_private_key(self, openssl_private_key_path):
        # openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
        keygen_args_genrsa = [
            "openssl",
            "genrsa",
            "2048",
        ]
        keygen_args_openssl = [
            "openssl",
            "pkcs8",
            "-topk8",
            "-inform",
            "PEM",
            "-out",
            openssl_private_key_path,
            "-nocrypt",
        ]
        try:
            ps = subprocess.Popen(keygen_args_genrsa, stdout=subprocess.PIPE)
            subprocess.check_output(keygen_args_openssl, stdin=ps.stdout)
        except CalledProcessError as e:
            raise SetupSSHException(e.output)

    def gen_print_openssl_public_key(
        self, openssl_private_key_path, openssl_public_key_path, private_generated
    ):
        keygen_args = [
            "openssl",
            "rsa",
            "-in",
            openssl_private_key_path,
            "-pubout",
            "-out",
            openssl_public_key_path,
        ]

        openssl_public_output = run_and_capture(keygen_args)
        if openssl_public_output.returncode != 0:
            if private_generated:
                raise SetupSSHException(openssl_public_output.stderr)
            else:
                raise ValueError(
                    "The private key provided can't be used to generate public RSA openssl keys."
                )

        console.print(f"\nOpenSSL public key saved at {openssl_public_key_path}")
        console.print(
            "Please configure the following key (yellow text) in services that require OpenSSL public keys to authenticate you (snowflake, etc.)\n"
        )
        openssl_public_key = open(openssl_public_key_path, "r").read()
        openssl_public_key = openssl_public_key.replace(
            "-----BEGIN PUBLIC KEY-----\n", ""
        ).replace("-----END PUBLIC KEY-----\n", "")
        console.print(f"[yellow]{openssl_public_key}[/yellow]")

    def gen_print_openssl_key(
        self, generate_private, openssl_private_key_path, openssl_public_key_path
    ):
        if generate_private:
            self.gen_openssl_private_key(openssl_private_key_path)
        self.gen_print_openssl_public_key(
            openssl_private_key_path, openssl_public_key_path, generate_private
        )

    def ssh_keygen_get_public_key(self, private_key_path):
        keygen_args = ["ssh-keygen", "-y", "-f", private_key_path]
        public_output = run_and_capture(keygen_args)

        public_type = public_output.stdout.split()[0]
        if public_output.stderr:
            raise SetupSSHException(public_output.stderr)
        return public_output.stdout, public_type

    def output_public_key_for_private(self, private_path_abs, public_key_path_abs):
        public_ssh_key, public_type = self.ssh_keygen_get_public_key(private_path_abs)
        with open(public_key_path_abs, "w") as file:
            file.write(public_ssh_key)
        openssl_private_path = private_path_abs if public_type == "ssh-rsa" else None
        return self.output_public_keys(public_key_path_abs, openssl_private_path)

    def output_public_keys(self, public_key_path_abs, openssl_private_path=None):
        openssl = self.get_config_value("open_ssl_public_key")
        if openssl:
            openssl_private_key_path = (
                openssl_private_path or f"{self.ssh_keys_dir_abs}/rsa_key.p8"
            )
            openssl_public_key_path = f"{self.ssh_keys_dir_abs}/rsa_key.pub"
            self.gen_print_openssl_key(
                openssl_private_path is None,
                openssl_private_key_path,
                openssl_public_key_path,
            )
        console.print(
            "Please configure the following key (yellow text) in your Git server (Gitlab, Github, Bitbucket, etc):\n"
        )
        console.print(f"[yellow]{open(public_key_path_abs, 'r').read()}[/yellow]")
        return questionary.confirm(
            "Have you configured your services and Git server with the keys above?"
            if openssl
            else "Have you configured your Git server with the key above?"
        ).ask()

    def get_config_value(self, key):
        return self.coves_config.integrated["setup"][self.args.task][key]
