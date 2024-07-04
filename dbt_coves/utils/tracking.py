import base64
import os

from dbt import version as dbt_version
from mixpanel import Mixpanel, MixpanelException

from dbt_coves import __version__ as dbt_coves_version
from dbt_coves.utils.log import LOGGER as logger

MIXPANEL_DEV_ENV = "M2JhMzA2MjE3ZDI5OGNjM2IxYTFmZmUzNDQyZTkzOGM="
MIXPANEL_PROD_ENV = "NjAyZTc1YWY4MGE1YzZkMTAzYzgzYmQyYzY3NWIyNDc="


def _get_mixpanel_env_token():
    is_dev = os.environ.get("DBTCOVES_DEV_ENV")
    token = MIXPANEL_DEV_ENV if is_dev else MIXPANEL_PROD_ENV
    return base64.b64decode(token).decode()


def trackable(task, **kwargs):
    def wrapper(task_instance, **kwargs):
        exit_code = task(task_instance)
        if task_instance.args.uuid and not (
            task_instance.args.disable_tracking or task_instance.coves_config.disable_tracking
        ):
            try:
                task_execution_props = _gen_task_usage_props(task_instance, exit_code)
                mixpanel = Mixpanel(token=_get_mixpanel_env_token())
                mixpanel.track(
                    distinct_id=task_instance.args.uuid,
                    event_name=f"{task_execution_props['dbt-coves command']}\
                        {task_execution_props.get('dbt-coves subcommand', '')}",
                    properties=task_execution_props,
                )
            except MixpanelException:
                logger.debug(
                    f"Unable to track task {task_instance} from user {task_instance.args.uuid}"
                )
        return exit_code

    return wrapper


def _gen_task_usage_props(task_instance, exit_code=1):
    usage_props = {}
    dbt_coves_command = task_instance.args.cls.__module__.split(".")[2]
    dbt_coves_subcommand = task_instance.args.which
    usage_props["dbt version"] = dbt_version.get_installed_version().to_version_string(
        skip_matcher=True
    )
    try:
        usage_props["dbt adapter"] = task_instance.adapter.type()
    except Exception:
        pass
    usage_props["dbt-coves version"] = dbt_coves_version
    usage_props["dbt-coves command"] = dbt_coves_command
    if dbt_coves_command != dbt_coves_subcommand:
        usage_props["dbt-coves subcommand"] = dbt_coves_subcommand
    usage_props["Successful"] = "Yes" if exit_code == 0 else "No"
    return usage_props
