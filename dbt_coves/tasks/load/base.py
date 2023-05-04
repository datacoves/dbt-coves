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
        payload = {}
        manager = self.secrets_manager.lower()
        if manager == "datacoves":
            # Contact the secrets manager and retrieve Secrets
            secrets_url = os.getenv("DATACOVES__SECRETS_URL") or self.get_config_value(
                "secrets_url"
            )
            secrets_token = os.getenv("DATACOVES__SECRETS_TOKEN") or self.get_config_value(
                "secrets_token"
            )
            secrets_project = os.getenv("DATACOVES__SECRETS_PROJECT") or self.get_config_value(
                "secrets_project"
            )

            if not (secrets_url and secrets_token and secrets_project):
                raise LoadException(
                    "[b]secrets_url[/b], [b]secrets_project[/b] and [b]secrets_token[/b] must "
                    "be provided when using a Secrets Manager"
                )

            secrets_url = f"{secrets_url}/api/v1/secrets/{secrets_project}"
            secrets_tags = self.get_config_value("secrets_tags")
            secrets_key = self.get_config_value("secrets_key")
            if secrets_tags:
                payload["tags"] = set(secrets_tags)
            if secrets_key:
                payload["key"] = secrets_key
            headers = {"Authorization": f"token {secrets_token}"}
            response = requests.get(secrets_url, headers=headers, verify=False, params=payload)
            response.raise_for_status()
            return response.json()

        raise LoadException(f"'{manager}' not recognized as a valid secrets manager.")
