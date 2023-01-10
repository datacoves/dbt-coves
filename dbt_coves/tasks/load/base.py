import glob
import json
import os
from pathlib import Path

import requests
from rich.console import Console

from dbt_coves.tasks.base import NonDbtBaseTask

console = Console()


class LoadException(Exception):
    pass


class BaseLoadTask(NonDbtBaseTask):
    def __init__(self, args, config):
        super().__init__(args, config)

    def retrieve_all_jsons_from_path(self, path):
        jsons = []

        for file in glob.glob(path + "/*.json"):
            filepath = Path(file)
            if filepath.stat().st_size > 0:
                with open(filepath, "r") as json_file:
                    jsons.append(json.load(json_file))
        return jsons

    def _load_secret_data(self) -> dict:
        # Contact the manager and retrieve Service Credentials
        secrets_url = os.getenv("DBT_COVES_SECRETS_URL") or self.get_config_value("secrets_url")
        secrets_token = os.getenv("DBT_COVES_SECRETS_TOKEN") or self.get_config_value(
            "secrets_token"
        )
        if not (secrets_url and secrets_token):
            raise LoadException(
                "[b]secrets_url[/b] and [b]secrets_token[/b] must be provided"
                "when using a Secrets Manager"
            )
        if self.secrets_manager.lower() == "datacoves":
            secrets_project = self.get_config_value("secrets_project")
            if not secrets_project:
                raise LoadException(
                    "[b]secrets_project[/b] must be provided when using 'datacoves'"
                    "as your Secrets Manager "
                )
            secrets_url = f"{secrets_url}/api/secrets/{secrets_project}"
            secrets_tags = self.get_config_value("secrets_tags")
            if secrets_tags:
                secrets_url += f"?tags={'&'.join(secrets_tags)}"

            # https://api.datacoves.localhost/api/secrets/analytics-local?tags=airbyte&fivetran

        headers = {"Authorization": f"token {secrets_token}"}
        response = requests.get(secrets_url, headers=headers, verify=False)
        response.raise_for_status()
        return response.json()
