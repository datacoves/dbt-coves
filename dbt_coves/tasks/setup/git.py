import os
from pathlib import Path
from urllib.parse import urlparse

import questionary
from rich.console import Console

from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.shell import run, run_and_capture
from .utils import print_row

console = Console()


class SetupGitException(Exception):
    pass


class SetupGitTask(NonDbtBaseTask):
    """
    Task that runs ssh key generation, git repo clone and db connection setup
    """

    key_column_with = 50
    value_column_with = 30

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "git",
            parents=[base_subparser],
            help="Set up Git repository of dbt project",
        )
        subparser.add_argument(
            "--no-prompt",
            help="Configure Git without user intervention",
            action="store_true",
            default=False,
        )
        subparser.set_defaults(cls=cls, which="git")
        return subparser

    def run(self, workspace_path=Path.cwd()) -> int:
        self._run_git_config()
        self._run_git_clone(workspace_path)
        return 0

    def _run_git_config(self):
        config_status = "[red]MISSING[/red]"

        email_output = run_and_capture(
            ["git", "config", "--global", "--get", "user.email"]
        )
        email_exists = email_output.returncode is 0 and email_output.stdout
        email = email_output.stdout.replace("\n", "")

        name_output = run_and_capture(
            ["git", "config", "--global", "--get", "user.name"]
        )
        name_exists = name_output.returncode is 0 and name_output.stdout
        name = name_output.stdout.replace("\n", "")
        if email_exists and name_exists:
            config_status = "[green]FOUND :heavy_check_mark:[/green]"
        print_row("Checking git config", config_status, new_section=True)
        if name:
            print_row(" - user.name ", name)
        if email:
            print_row(" - user.email ", email)

        if not email_exists or not name_exists:
            name = ""
            email = ""
            no_prompt = self.get_config_value("no_prompt")
            if no_prompt:
                name = os.environ.get("USER_FULLNAME", "")
                email = os.environ.get("USER_EMAIL", "")
                if not (name and email):
                    raise SetupGitException(
                        f"[yellow]USER_FULLNAME ({name or 'missing'})[/yellow] and [yellow]USER_EMAIL ({email or 'missing'})[/yellow] environment variables must be set in order to setup Git with [i]--no-prompt[/i]"
                    )
            else:
                default_name = os.environ.get("USER_FULLNAME", "")
                name = questionary.text(
                    "Please type your full name:", default=default_name
                ).ask()
                if name:
                    default_email = os.environ.get("USER_EMAIL", "")
                    email = questionary.text(
                        "Please type your email address:", default=default_email
                    ).ask()
            if name and email:
                name_output = run_and_capture(
                    ["git", "config", "--global", "user.name", name]
                )
                if name_output.returncode is not 0:
                    console.print("Could not set user.name")
                    return 1
                email_output = run_and_capture(
                    ["git", "config", "--global", "user.email", email]
                )
                if email_output.returncode is not 0:
                    console.print("Could not set user.email")
                    return 1
                console.print(
                    "[green]:heavy_check_mark: Git user configured successfully."
                )

    def _run_git_clone(self, workspace_path):
        repo_url = ""
        cloned_status = "[red]MISSING[/red]"
        cloned_exists = Path(workspace_path, ".git").exists()
        if cloned_exists:
            cloned_status = "[green]FOUND :heavy_check_mark:[/green]"
        print_row(f"Checking for git repo", cloned_status, new_section=True)

        if cloned_exists:
            return

        if any(os.scandir(workspace_path)):
            console.print(f"Folder '{workspace_path}' is not empty.")
            return

        no_prompt = self.get_config_value("no_prompt")
        if no_prompt:
            repo_url = os.environ.get("GIT_REPO_URL", "")
            if not repo_url:
                raise SetupGitException(
                    f"[yellow]GIT_REPO_URL[/yellow] environment variable must be set in order to clone Git repository with [i]--no-prompt[/i]"
                )
        else:
            default_repo_url = os.environ.get("GIT_REPO_URL", "")
            repo_url = questionary.text(
                "Please type the git repo SSH url:", default=default_repo_url
            ).ask()

        if repo_url:
            ssh_repo_url = f"ssh://{repo_url}" if "ssh://" not in repo_url else repo_url
            url_parsed = urlparse(ssh_repo_url)
            domain = url_parsed.hostname
            port = None
            try:
                port = url_parsed.port
            except ValueError:
                pass
            if port:
                output = run_and_capture(
                    ["ssh-keyscan", "-t", "rsa", "-p", str(port), domain]
                )
            else:
                output = run_and_capture(["ssh-keyscan", "-t", "rsa", domain])

            if output.returncode != 0:
                raise Exception(f"Failed to run ssh-keyscan. {output.stderr}")

            new_host = output.stdout
            known_hosts_path = Path("~/.ssh/known_hosts").expanduser()
            if not known_hosts_path.exists():
                known_hosts_path.parent.mkdir(parents=True, exist_ok=True)
                open(known_hosts_path, "w")

            hosts = open(known_hosts_path, "r").read()
            if domain not in hosts:
                with open(known_hosts_path, "a") as file:
                    file.write(new_host)
                console.print(
                    f"[green]:heavy_check_mark: {domain} registared as a SSH known host."
                )

            if output.returncode is 0:
                output = run(["git", "clone", repo_url, workspace_path])
                if output.returncode is 0:
                    console.print(
                        f"[green]:heavy_check_mark: Repo cloned successfully on '{workspace_path}'"
                    )
                else:
                    raise Exception(f"Failed to clone git repo '{repo_url}'")
            else:
                raise Exception(
                    f"Failed to clone git repo '{repo_url}': {output.stderr}"
                )

    def get_config_value(self, key):
        return self.coves_config.integrated["setup"]["git"][key]
