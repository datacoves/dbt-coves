import os

import requests

from dbt_coves.core.exceptions import DbtCovesException


def load_secret_manager_data(task_instance) -> dict:
    payload = {}
    manager = task_instance.secrets_manager.lower()
    if manager == "datacoves":
        # Contact the secrets manager and retrieve Secrets
        secrets_url = os.getenv("DATACOVES__SECRETS_URL") or task_instance.get_config_value(
            "secrets_url"
        )
        secrets_token = os.getenv("DATACOVES__SECRETS_TOKEN") or task_instance.get_config_value(
            "secrets_token"
        )
        secrets_project = os.getenv("DATACOVES__SECRETS_PROJECT") or task_instance.get_config_value(
            "secrets_project"
        )

        if not (secrets_url and secrets_token and secrets_project):
            raise DbtCovesException(
                "[b]secrets_url[/b], [b]secrets_project[/b] and [b]secrets_token[/b] must "
                "be provided when using a Secrets Manager"
            )

        secrets_url = f"{secrets_url}/api/v1/secrets/{secrets_project}"
        secrets_tags = task_instance.get_config_value("secrets_tags")
        secrets_key = task_instance.get_config_value("secrets_key")
        if secrets_tags:
            if isinstance(secrets_tags, str):
                payload["tags"] = [secrets_tags]
            else:
                payload["tags"] = set(secrets_tags)
        if secrets_key:
            payload["key"] = secrets_key
        headers = {"Authorization": f"token {secrets_token}"}
        response = requests.get(secrets_url, headers=headers, verify=False, params=payload)
        response.raise_for_status()
        return response.json()

    raise DbtCovesException(f"'{manager}' not recognized as a valid secrets manager.")
