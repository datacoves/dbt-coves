import os
import re
from typing import Optional

import requests

from dbt_coves.core.exceptions import DbtCovesException

SECRET_PATTERN = re.compile(r"\{\{\s*secret\('([^']+)'\)\s*\}\}", re.IGNORECASE)

# Datacoves secrets manager settings, as (config key, environment variable) pairs
DATACOVES_SECRETS_SETTINGS = (
    ("secrets_url", "DATACOVES__SECRETS_URL"),
    ("secrets_token", "DATACOVES__SECRETS_TOKEN"),
    ("secrets_environment", "DATACOVES__ENVIRONMENT_SLUG"),
)


def contains_secret(value) -> bool:
    """
    Whether `value` holds a `{{ secret('...') }}` reference anywhere inside it
    """
    if isinstance(value, dict):
        return any(contains_secret(item) for item in value.values())
    if isinstance(value, list):
        return any(contains_secret(item) for item in value)
    if isinstance(value, str):
        return bool(SECRET_PATTERN.search(value))
    return False


def load_secret_manager_data(task_instance) -> dict:
    payload = {}
    manager = task_instance.secrets_manager.lower()
    if manager == "datacoves":
        # Contact the secrets manager and retrieve Secrets
        settings = {
            key: os.getenv(env_var) or task_instance.get_config_value(key)
            for key, env_var in DATACOVES_SECRETS_SETTINGS
        }
        missing = [
            f"[b]{key}[/b] (or [b]{env_var}[/b])"
            for key, env_var in DATACOVES_SECRETS_SETTINGS
            if not settings[key]
        ]
        if missing:
            raise DbtCovesException(
                f"{', '.join(missing)} must be provided when using a Secrets Manager"
            )
        secrets_token = settings["secrets_token"]
        secrets_environment = settings["secrets_environment"]

        secrets_url = f"{settings['secrets_url']}/api/v1/secrets/{secrets_environment}"
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
        response = requests.get(secrets_url, headers=headers, params=payload)
        response.raise_for_status()
        return response.json()

    raise DbtCovesException(f"'{manager}' not recognized as a valid secrets manager.")


def _replace_value_secrets(secrets_list, value, errors: set):
    """
    Return `value` with a `secret()` reference it holds replaced by the secret's
    value -- a string carrying one is replaced whole, so the secret's value can
    be of any type -- collecting unresolvable references in `errors`
    """
    if isinstance(value, dict):
        for key, item in value.items():
            value[key] = _replace_value_secrets(secrets_list, item, errors)
        return value
    if isinstance(value, list):
        for index, item in enumerate(value):
            value[index] = _replace_value_secrets(secrets_list, item, errors)
        return value
    if isinstance(value, str):
        value_secret = SECRET_PATTERN.search(value)
        if value_secret:
            secret_key = value_secret.group(1)
            secret_found = False
            for secret in secrets_list:
                if secret.get("slug", "").lower() == secret_key.lower():
                    secret_found = True
                    if secret.get("slug", "") == secret_key:
                        return secret.get("value")
                    errors.add(
                        f"Secret [red]{secret_key}[/red] not found in secrets, "
                        f"did you mean [red]{secret.get('slug')}[/red]"
                    )
            if not secret_found:
                errors.add(f"Secret [red]{secret_key}[/red] not found in secrets")
    return value


def replace_secrets(secrets_list, dictionary, errors: Optional[set] = None):
    if errors is None:
        errors = set()
    _replace_value_secrets(secrets_list, dictionary, errors)
    if errors:
        error_message = "Errors found:\n"
        error_message += "\n".join(errors)
        raise DbtCovesException(error_message)
