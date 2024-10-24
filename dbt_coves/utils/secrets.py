import os
import re

import requests

from dbt_coves.core.exceptions import DbtCovesException

SECRET_PATTERN = re.compile(r"\{\{\s*secret\('([^']+)'\)\s*\}\}", re.IGNORECASE)


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
        secrets_environment = os.getenv(
            "DATACOVES__ENVIRONMENT_SLUG"
        ) or task_instance.get_config_value("secrets_environment")
        if not (secrets_url and secrets_token and secrets_environment):
            raise DbtCovesException(
                "[b]secrets_url[/b], [b]secrets_environment[/b] and [b]secrets_environment[/b] must "
                "be provided when using a Secrets Manager"
            )

        secrets_url = f"{secrets_url}/api/v1/secrets/{secrets_environment}"
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


def replace_secrets(secrets_list, dictionary, errors: set = set()):
    for key, value in dictionary.items():
        if isinstance(value, dict):
            replace_secrets(secrets_list, value, errors)
        elif isinstance(value, str):
            value_secret = SECRET_PATTERN.search(value)
            if value_secret:
                secret_key = value_secret.group(1)
                secret_found = False
                for secret in secrets_list:
                    if secret.get("slug", "").lower() == secret_key.lower():
                        secret_found = True
                        if secret.get("slug", "") == secret_key:
                            dictionary[key] = secret.get("value")
                        else:
                            errors.add(
                                f"Secret [red]{secret_key}[/red] not found in secrets, "
                                f"did you mean [red]{secret.get('slug')}[/red]"
                            )
                if not secret_found:
                    errors.add(f"Secret [red]{secret_key}[/red] not found in secrets")
    if errors:
        error_message = "Errors found:\n"
        error_message += "\n".join(errors)
        raise DbtCovesException(error_message)
