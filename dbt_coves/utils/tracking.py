import os

from dbt import version as dbt_version
from mixpanel import Mixpanel

from dbt_coves import __version__ as dbt_coves_version

MIXPANEL_DEV_TOKEN = os.environ.get("MIXPANEL_DEV_TOKEN", "3ba306217d298cc3b1a1ffe3442e938c")
MIXPANEL_PROD_TOKEN = os.environ.get("MIXPANEL_PROD_TOKEN", "602e75af80a5c6d103c83bd2c675b247")


def _get_mixpanel_env_token():
    is_dev = os.environ.get("DBTCOVES_DEV_ENV")
    return MIXPANEL_DEV_TOKEN if is_dev else MIXPANEL_PROD_TOKEN


def trackable(task, **kwargs):
    def wrapper(task_instance, **kwargs):
        exit_code = task(task_instance)
        if not task_instance.args.disable_tracking:
            task_execution_props = _gen_task_usage_props(task_instance, exit_code)
            mixpanel = Mixpanel(token=_get_mixpanel_env_token())
            mixpanel.track(
                distinct_id=task_instance.args.uuid,
                event_name=f"{task_execution_props['dbt-coves command']}\
                      {task_execution_props.get('dbt-coves subcommand', '')}",
                properties=task_execution_props,
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
