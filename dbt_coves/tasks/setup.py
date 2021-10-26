import os
from pathlib import Path
from urllib.parse import urlparse

import questionary
from rich.table import Table
from rich.console import Console
from jinja2 import Environment, meta

from dbt_coves.utils.jinja import render_template
from dbt_coves.utils.shell import run_and_capture, run
from .base import BaseTask
from dbt_coves.config.config import DbtCovesConfig

console = Console()


class SetupTask(BaseTask):
    """
    Task that runs ssh key generation, git repo clone and db connection setup
    """

    key_column_with = 50
    value_column_with = 30

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "setup",
            parents=[base_subparser],
            help="Sets up SSH keys, git repo, and db connections.",
        )
        subparser.set_defaults(cls=cls, which="setup")
        return subparser

    def run(self) -> int:
        """
        Env vars that can be set: USER_FULLNAME, USER_EMAIL, WORKSPACE_PATH, GIT_REPO_URL, DBT_PROFILES_DIR
        """
        workspace_path = os.environ.get("WORKSPACE_PATH", Path.cwd())

        self.ssh_key()
        self.git_config()
        self.git_clone(workspace_path)

        config_folder = DbtCovesConfig.get_config_folder(workspace_path=workspace_path)

        context = self.dbt_profiles(config_folder)
        self.dbt_debug(config_folder)
        self.vs_code(workspace_path, config_folder, context)

        return 0

    def vs_code(self, workspace_path, config_folder, prev_context):
        template_path = Path(config_folder, "templates", "settings.json")
        if not template_path.exists():
            return

        code_local_path = Path(workspace_path, ".vscode", "settings.json")
        sqltools_status = "[red]MISSING[/red]"
        settings_exists = code_local_path.exists()
        if settings_exists:
            sqltools_status = "[green]FOUND :heavy_check_mark:[/green]"
        self.print_row(
            f"Checking for vs code settings",
            sqltools_status,
            new_section=True,
        )
        if not settings_exists:
            template_text = open(template_path, "r").read()
            self.print_row(" - settings.json template", "OK")

            env = Environment()
            parsed_content = env.parse(template_text)
            context = prev_context or dict()
            for key in meta.find_undeclared_variables(parsed_content):
                if key not in context:
                    if "password" in key or "token" in key:
                        value = questionary.password(f"Please enter {key}:").ask()
                    else:
                        value = questionary.text(f"Please enter {key}:").ask()
                    context[key] = value
            new_settings = render_template(template_text, context)
            path = Path(workspace_path, ".vscode")
            path.mkdir(parents=True, exist_ok=True)

            with open(code_local_path, "w") as file:
                file.write(new_settings)
            console.print(
                f"[green]:heavy_check_mark: vs code settings successfully generated in {code_local_path}."
            )

    def dbt_debug(self, config_folder):
        debug_status = "[red]FAIL[/red]"
        console.print("\n")
        output = run(["dbt", "debug"], cwd=config_folder.parent)
        if output.returncode is 0:
            debug_status = "[green]SUCCESS :heavy_check_mark:[/green]"
        self.print_row(
            "dbt debug",
            debug_status,
            new_section=True,
        )
        if output.returncode > 0:
            raise Exception("dbt debug error. Check logs.")

    def dbt_profiles(self, config_folder):
        profiles_status = "[red]MISSING[/red]"
        default_dbt_path = Path("~/.dbt").expanduser()
        dbt_path = os.environ.get("DBT_PROFILES_DIR", default_dbt_path)
        profiles_path = Path(dbt_path, "profiles.yml")
        profiles_exists = profiles_path.exists()
        if profiles_exists:
            profiles_status = "[green]FOUND :heavy_check_mark:[/green]"
        self.print_row(
            f"Checking for profiles.yml in '{dbt_path}'",
            profiles_status,
            new_section=True,
        )

        if profiles_exists:
            return None

        template_path = Path(config_folder, "templates", "profiles.yml")
        try:
            template_text = open(template_path, "r").read()
            self.print_row(" - profiles.yml template", "OK")
        except FileNotFoundError:
            raise Exception(
                f"Could not genereate Dbt profile. Template not found in '{template_path}'"
            )

        env = Environment()
        parsed_content = env.parse(template_text)
        context = dict()
        for key in meta.find_undeclared_variables(parsed_content):
            if "password" in key or "token" in key:
                value = questionary.password(f"Please enter {key}:").ask()
            else:
                value = questionary.text(f"Please enter {key}:").ask()
            context[key] = value
        new_profiles = render_template(template_text, context)
        profiles_path.parent.mkdir(parents=True, exist_ok=True)
        with open(profiles_path, "w") as file:
            file.write(new_profiles)
        console.print(
            f"[green]:heavy_check_mark: dbt profiles successfully generated in {profiles_path}."
        )
        return context

    def git_clone(self, workspace_path):
        cloned_status = "[red]MISSING[/red]"
        cloned_exists = Path(workspace_path, ".git").exists()
        if cloned_exists:
            cloned_status = "[green]FOUND :heavy_check_mark:[/green]"
        self.print_row(f"Checking for git repo", cloned_status, new_section=True)

        if cloned_exists:
            return

        if any(os.scandir(workspace_path)):
            raise Exception(f"Folder '{workspace_path}' is not empty.")

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

    def git_config(self):
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
        self.print_row("Checking git config", config_status, new_section=True)
        if name:
            self.print_row(" - user.name ", name)
        if email:
            self.print_row(" - user.email ", email)

        if not email_exists or not name_exists:
            default_name = os.environ.get("USER_FULLNAME", "")
            new_name = questionary.text(
                "Please type your full name:", default=default_name
            ).ask()
            if new_name:
                default_email = os.environ.get("USER_EMAIL", "")
                new_email = questionary.text(
                    "Please type your email address:", default=default_email
                ).ask()
                if new_email:
                    name_output = run_and_capture(
                        ["git", "config", "--global", "user.name", new_name]
                    )
                    if name_output.returncode is not 0:
                        console.print("Could not set user.name")
                        return 1
                    email_output = run_and_capture(
                        ["git", "config", "--global", "user.email", new_email]
                    )
                    if email_output.returncode is not 0:
                        console.print("Could not set user.email")
                        return 1
                    console.print(
                        "[green]:heavy_check_mark: Git user configured successfully."
                    )

    def print_row(
        self, key, value="[green]FOUND :heavy_check_mark:[/green]", new_section=False
    ):
        grid = Table.grid(expand=False)
        grid.add_column(width=self.key_column_with)
        grid.add_column(justify="right", width=self.value_column_with)
        grid.add_row(key, value)
        if new_section:
            console.print("\n")
        console.print(grid)

    def ssh_key(self):
        ssh_status = "[red]MISSING[/red]"
        key_path = "~/.ssh/id_rsa"
        key_path_abs = Path(key_path).expanduser()
        ssh_exists = key_path_abs.exists()
        if ssh_exists:
            ssh_status = "[green]FOUND :heavy_check_mark:[/green]"
        self.print_row(
            f"Checking for key in '{key_path}'", ssh_status, new_section=True
        )
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
