import datetime
import importlib
import os
import re
import textwrap
from glob import glob
from pathlib import Path
from typing import Any, Dict

import isort
import yaml
from black import FileMode, format_str
from rich.console import Console
from rich.markup import escape

from dbt_coves.core.exceptions import DbtCovesException, MissingArgumentException
from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.secrets import (
    contains_secret,
    load_secret_manager_data,
    replace_secrets,
)
from dbt_coves.utils.tracking import trackable
from dbt_coves.utils.yaml import deep_merge

console = Console()

AIRFLOW_K8S_CONFIG_TEMPLATE = textwrap.dedent(
    """{{
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name='base',
                        {config}
                    )
                ]
            )
        ),
}}"""
)


class GenerateAirflowDagsException(Exception):
    pass


AIRFLOW_VERSIONS = (2, 3)

# Where each Airflow major version imports the @dag, @task and @task_group
# decorators from. airflow.sdk doesn't exist in Airflow 2, and airflow.decorators is
# deprecated in Airflow 3.
DECORATORS_MODULE = {2: "airflow.decorators", 3: "airflow.sdk"}

# DAG arguments only one of the two versions accepts, from the DAG constructor of
# Airflow 2.10 and of Airflow 3's task SDK: per target version, the ones with a
# direct replacement (renamed when generating) and the ones without (an error).
DAG_ARG_RENAMES = {
    3: {
        "schedule_interval": "schedule",
        "timetable": "schedule",
        "fail_stop": "fail_fast",
        "concurrency": "max_active_tasks",
    },
    2: {"fail_fast": "fail_stop"},
}
UNSUPPORTED_DAG_ARGS = {
    3: {"default_view", "orientation", "full_filepath"},
    2: {
        "allowed_run_types",
        "deadline",
        "disable_bundle_versioning",
        "rerun_with_latest_version",
        "task_group",
    },
}

# Core operator and sensor modules Airflow 3 moved to the
# apache-airflow-providers-standard package; the old paths are deprecated.
AIRFLOW_3_MOVED_MODULES = ("airflow.operators", "airflow.sensors")

_IMPORT_STATEMENT = re.compile(r"^\s*(?:from|import)\s+([\w.]+)")


def _referenced_module(reference: str) -> str:
    """
    The module an import statement (`from x.y import z`, `import x.y`) refers to,
    or the dotted path itself for an operator path (`x.y.Class`).
    """
    match = _IMPORT_STATEMENT.match(reference)
    return match.group(1) if match else reference.strip()


def _in_module(path: str, module: str) -> bool:
    """Whether a dotted path is `module` or inside it (not just similarly named)."""
    return path == module or path.startswith(f"{module}.")


class RawExpr(str):
    """A string marked via the `!py` YAML tag to be emitted as a raw, unquoted Python expression."""

    def __repr__(self):
        # Dicts and lists render through their elements' repr, so this is what
        # keeps a `!py` value nested inside one unquoted too
        return str(self)


class GenerateAirflowDagsTask(NonDbtBaseTask):
    """
    Task that generate sources, models and model properties automatically
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "airflow-dags",
            parents=[base_subparser],
            help="Generate Airflow Python DAGs from YML configuration files",
        )
        subparser.add_argument(
            "--airflow-version",
            type=int,
            choices=AIRFLOW_VERSIONS,
            help="Major version of Airflow the DAGs are for (2 or 3). Required: "
            "DAGs generated for one version don't work on the other",
        )
        subparser.add_argument(
            "--yml-path",
            "--yaml-path",
            type=str,
            help="Folder where YML files will be read from",
        )
        subparser.add_argument(
            "--dags-path",
            type=str,
            help="Folder where generated Python files will be stored",
        )
        subparser.add_argument(
            "--validate-operators",
            help="Ensure Airflow operators are installed by trying to import them "
            "prior to writing them with `generate airflow-dags`",
            action="store_true",
            default=False,
        )
        subparser.add_argument(
            "--generators-folder",
            type=str,
            help="Custom DAG generators folder",
        )
        subparser.add_argument(
            "--generators-params",
            help="Object with default values for the desired Generator(s), i.e "
            "{'AirbyteDbtGenerator' {'host': 'http://localhost'}}",
            type=str,
        )
        subparser.add_argument(
            "--secrets-path",
            type=str,
            help="Secret files location for DAG configuration, i.e. './secrets'",
        )
        subparser.add_argument(
            "--secrets-manager",
            type=str,
            help="Secret credentials provider, i.e. 'datacoves'",
        )
        subparser.add_argument(
            "--secrets-url", type=str, help="Secret credentials provider url"
        )
        subparser.add_argument(
            "--secrets-token", type=str, help="Secret credentials provider token"
        )
        subparser.add_argument(
            "--secrets-environment", type=str, help="Secret credentials project"
        )
        subparser.add_argument(
            "--secrets-tags", type=str, help="Secret credentials tags"
        )
        subparser.add_argument("--secrets-key", type=str, help="Secret credentials key")

        cls.arg_parser = base_subparser
        subparser.set_defaults(cls=cls, which="airflow_dags")
        return subparser

    def __init__(self, args, config):
        super().__init__(args, config)

    # Custom constructor to convert to datetime.datetime
    def date_constructor(self, loader, node):
        value = loader.construct_scalar(node)
        return datetime.datetime.strptime(value, "%Y-%m-%d")

    # Custom constructor for `!py` tagged values: emitted as raw Python expressions
    def raw_expr_constructor(self, loader, node):
        return RawExpr(loader.construct_scalar(node))

    def get_config_value(self, key):
        return self.coves_config.integrated["generate"]["airflow_dags"][key]

    def _generate_dag(self, yml_filepath: Path):
        yaml.FullLoader.add_constructor(
            "tag:yaml.org,2002:timestamp", self.date_constructor
        )
        yaml.FullLoader.add_constructor("!py", self.raw_expr_constructor)
        console.print(f"Generating [b][i]{yml_filepath.stem}[/i][/b]")
        try:
            if self.dags_path:
                if yml_filepath != self.ymls_path:
                    yml_relpath = yml_filepath.relative_to(self.ymls_path)
                elif self.yml_dags_path_env:
                    yml_relpath = yml_filepath.relative_to(
                        Path(f"/config/workspace/{self.yml_dags_path_env}")
                    )
                else:
                    yml_relpath = yml_filepath.name
                dag_destination = (
                    Path(self.dags_path)
                    .resolve()
                    .joinpath(yml_relpath)
                    .with_suffix(".py")
                )
            else:
                dag_destination = yml_filepath.with_suffix(".py")
            dag_destination.parent.mkdir(parents=True, exist_ok=True)
            self.build_dag_file(
                destination_path=dag_destination,
                dag_name=yml_filepath.stem,
                yml_dag=yaml.full_load(open(yml_filepath)),
            )
        # A DAG's own problem -- unreadable YML, an unresolvable secret, a
        # generator that can't reach its API -- skips that DAG, it doesn't stop
        # the ones still to be generated
        except (GenerateAirflowDagsException, DbtCovesException) as e:
            self._skip_dag(yml_filepath, str(e))  # our own messages hold markup
        except Exception as e:
            # Anything else is unexpected, so name the type: this is all the
            # caller gets to tell a DAG's own problem from a bug in here
            self._skip_dag(
                yml_filepath,
                f"DAG [red][b][i]{yml_filepath.stem}[/i][/b][/red] could not be "
                f"generated. {type(e).__name__}: {escape(str(e))}",
            )

    def _skip_dag(self, yml_filepath: Path, message: str):
        console.print(f"[red]{message}[/red]")
        self.skipped_dags.append(yml_filepath.stem)

    def _get_airflow_version(self) -> int:
        configured = self.get_config_value("airflow_version")
        version = None
        if type(configured) is int:  # not bool, nor a float like 3.0
            version = configured
        elif isinstance(configured, str) and configured.strip() in ("2", "3"):
            # From config.yml it can be a string, e.g. from `{{ env_var(...) }}`
            version = int(configured.strip())
        if version not in AIRFLOW_VERSIONS:
            given = f" (got {configured!r})" if configured is not None else ""
            raise DbtCovesException(
                f"[red]generate airflow-dags needs the major version of Airflow the "
                f"DAGs are for{given}: 2 or 3.[/red] Pass "
                f"[b]--airflow-version 2[/b] or [b]--airflow-version 3[/b], or set "
                f"[b]generate.airflow_dags.airflow_version[/b] in "
                f"[yellow].dbt_coves/config.yml[/yellow]. DAGs generated for one "
                f"version don't work on the other (e.g. Airflow 3 imports @dag from "
                f"airflow.sdk, which Airflow 2 doesn't have)."
            )
        return version

    @trackable
    def run(self):
        self.airflow_version = self._get_airflow_version()
        ymls_path = self.get_config_value("yml_path")
        self.dags_path = self.get_config_value("dags_path")
        if not (ymls_path):
            raise MissingArgumentException(["--yml-path"], self.coves_config)
        self.validate_operators = self.get_config_value("validate_operators")
        self.secrets_path = self.get_config_value("secrets_path")
        self.secrets_manager = self.get_config_value("secrets_manager")
        self.secret_data = None
        self.secret_data_error = None
        self.skipped_dags = []
        self.warned_airflow_3_moved: set = set()
        self.yml_dags_path_env = os.environ.get("DATACOVES__AIRFLOW_DAGS_YML_PATH")

        if self.secrets_path and self.secrets_manager:
            raise GenerateAirflowDagsException(
                "Can't use 'secrets_path' and 'secrets_manager' simultaneously."
            )
        self.ymls_path = Path(ymls_path).resolve()
        if self.ymls_path.is_dir():
            for yml_filepath in glob(f"{self.ymls_path}/**/*.yml", recursive=True):
                self._generate_dag(Path(yml_filepath))
        else:
            self._generate_dag(self.ymls_path)
        if self.skipped_dags:
            # Skipped DAGs leave whatever was at their destination in place, so
            # the run has to fail for a caller to notice they're now stale
            console.print(
                f"[red]Skipped {len(self.skipped_dags)} DAG(s): "
                f"{', '.join(self.skipped_dags)}[/red]"
            )
            return 1
        return 0

    def _register_datetime_import(self, value):
        """
        Values containing datetime.datetime/date/time instances render via
        their `datetime.xxx(...)` repr, so make sure the generated DAG file
        imports the `datetime` module whenever one shows up.
        """
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            self.dag_output["imports"].append("import datetime\n")
        elif isinstance(value, dict):
            for v in value.values():
                self._register_datetime_import(v)
        elif isinstance(value, (list, tuple)):
            for v in value:
                self._register_datetime_import(v)

    def _python_value(self, value):
        """
        Render a non-string argument as Python. A datetime interpolates through
        `str()` as `2023-01-01 00:00:00`, which doesn't parse, so it goes through
        `repr()` instead -- one nested in a dict or list already does, as their
        own repr renders their elements
        """
        self._register_datetime_import(value)
        if isinstance(value, (datetime.datetime, datetime.date, datetime.time)):
            return repr(value)
        return value

    def _warn_on_quoted_py_tag(self, key, value):
        """
        `key: '!py expr()'` is an ordinary string to YAML -- the tag has to sit
        outside the quotes to be one -- so the `!py` would be emitted verbatim
        """
        if value.startswith("!py "):
            console.print(
                f"[yellow]{key} starts with [b]!py[/b] inside its quotes, so it is a "
                f"plain string: move the tag outside them to emit an expression[/yellow]"
            )

    def dag_args_to_string(self, yaml, indent=2):
        """
        Converts a dictionary to a string of arguments for the DAG constructor.
        """
        dag_args = ""
        for key, value in yaml.items():
            if "notifications" in key:
                for call in self.generate_notifiers(yaml["notifications"]):
                    dag_args += f"{indent * ' '}{call},\n"
            else:
                if isinstance(value, RawExpr):
                    dag_value = str(value)
                elif isinstance(value, str) and "config" not in key:
                    self._warn_on_quoted_py_tag(key, value)
                    dag_value = f'"{value}"'
                else:
                    dag_value = self._python_value(value)
                dag_args += f"{indent * ' '}{key}={dag_value},\n"
        return dag_args[:-1]

    def generate_notifiers(self, notifiers: Dict[str, Any]):
        """
        Generate imports, globals, and return DAG `callback=Class(args=args)` settings
        """
        callback_output = []
        for callback, definition in notifiers.items():
            notifier = definition.get("notifier", definition.get("callback"))
            if not notifier:
                raise GenerateAirflowDagsException(
                    "Could not find a notifier or callback in the Notifications settings."
                )
            # Splitting into module and class
            # e.g. 'dbt_coves.notifications.slack.SlackNotifier'
            split_callback = notifier.split(".")
            module = ".".join(split_callback[:-1])
            callback_class = split_callback[-1]
            callback_args = definition.get("args")
            self.dag_output["imports"].append(
                f"from {module} import {callback_class}\n"
            )
            usage_args = []
            if isinstance(callback_args, dict):
                for arg, value in callback_args.items():
                    if isinstance(value, RawExpr):  # `!py` tagged: emit as written
                        value = str(value)
                    elif isinstance(value, str):
                        self._warn_on_quoted_py_tag(arg, value)
                        value = f'"{value}"'
                    else:
                        value = self._python_value(value)
                    usage_args.append(f"{arg}={value}")
            if isinstance(callback_args, list):
                for arg in callback_args:
                    if isinstance(arg, RawExpr):
                        usage_args.append(str(arg))
                    elif isinstance(arg, dict):
                        arg = self.dag_args_to_string(arg, indent=4).rstrip(",")
                        usage_args.append(arg)
                    elif isinstance(arg, int):
                        usage_args.append(f"{arg}")
                    elif isinstance(arg, str):
                        usage_args.append(f'"{arg}"')
            callback_usage = f"{callback_class}({','.join(usage_args)})"
            callback_output.append(f"{2 * ' '}{callback}={callback_usage}")
        return callback_output

    def build_dag_file(
        self, destination_path: Path, dag_name: str, yml_dag: Dict[str, Any]
    ):
        """
        Generate DAG Python file based on YML configuration
        """
        self.generated_groups = {}
        self.collected_dependencies = []
        yml_dag = self._discover_secrets(yml_dag)
        try:
            nodes = yml_dag.pop("nodes")
        except KeyError:
            raise GenerateAirflowDagsException(
                f"YML file [red][b][i]{dag_name}[/i][/b][/red] must contain a 'nodes' section"
            )
        extra_imports = yml_dag.pop("imports", [])
        for extra_import in extra_imports:
            self._check_airflow_version_compat(extra_import)
        yml_dag = self._adapt_dag_args(dag_name, yml_dag)
        doc_md = yml_dag.get("doc_md", None)
        self.dag_output = {
            "docstring": [],
            "imports": [
                f"from {self._decorators_module} import dag\n",
                *[f"{imp}\n" for imp in extra_imports],
            ],
            "globals": [],
            "dag": ["@dag(\n"],
        }
        if doc_md:
            self.dag_output["docstring"].append(f'"""\n{doc_md.rstrip()}\n"""\n\n')
            yml_dag["doc_md"] = RawExpr(
                "__doc__"
            )  # update in-place to preserve key order
        self.dag_output["dag"].extend(
            [
                f"{self.dag_args_to_string(yml_dag)}\n",
                ")\n",
                f"def {dag_name}():\n",
            ]
        )
        for node_name, node_conf in nodes.items():
            self.generate_node(node_name, node_conf)
        for upstream_list, task_name in self.collected_dependencies:
            self.dag_output["dag"].append(
                f"    [{', '.join(upstream_list)}] >> {task_name}\n"
            )
        self.dag_output["dag"].append(f"dag = {dag_name}()\n")

        final_output = (
            "".join(self.dag_output["docstring"])
            + "".join(set(self.dag_output["imports"]))
            + "".join(self.dag_output["globals"])
            + "".join(self.dag_output["dag"])
        )
        try:
            black_formatted = format_str(final_output, mode=FileMode())
            final_output = isort.code(black_formatted)
        except Exception as exc:
            # Only write once the output is known to be valid Python: a DAG we
            # can't generate must not overwrite the last one we could
            raise GenerateAirflowDagsException(
                f"DAG [red][b][i]{dag_name}[/i][/b][/red] resulted in an invalid DAG, "
                f"skipping. [b]{destination_path}[/b] left unchanged. "
                # The error echoes the source line it choked on, and a `[...]` in
                # it would otherwise be read as markup and dropped from the only
                # diagnostic there is now that nothing is written
                f"Error: {escape(str(exc))}"
            )
        destination_path.write_text(final_output)

    @property
    def _decorators_module(self) -> str:
        return DECORATORS_MODULE[self.airflow_version]

    def _adapt_dag_args(self, dag_name: str, yml_dag: Dict[str, Any]):
        """
        Rename DAG arguments the target Airflow version only knows under another
        name (e.g. `schedule_interval` is `schedule` in Airflow 3), keeping their
        order. Arguments with no equivalent in that version skip the DAG.
        """
        renames = DAG_ARG_RENAMES[self.airflow_version]
        unsupported = sorted(set(yml_dag) & UNSUPPORTED_DAG_ARGS[self.airflow_version])
        if unsupported:
            raise GenerateAirflowDagsException(
                f"DAG [red][b][i]{dag_name}[/i][/b][/red] uses "
                f"{', '.join(unsupported)}, which Airflow {self.airflow_version} "
                f"doesn't support and has no equivalent for: remove it"
            )
        adapted: Dict[str, Any] = {}
        renamed_from: Dict[str, str] = {}
        for key, value in yml_dag.items():
            new_key = renames.get(key, key)
            if new_key != key:
                # The other key is the user's: the new name itself, or another old
                # one renamed to it (schedule_interval and timetable -> schedule)
                other = new_key if new_key in yml_dag else renamed_from.get(new_key)
                if other:
                    raise GenerateAirflowDagsException(
                        f"DAG [red][b][i]{dag_name}[/i][/b][/red] sets both {other} and "
                        f"{key}: Airflow {self.airflow_version} only has {new_key}, "
                        f"keep one"
                    )
                renamed_from[new_key] = key
                # fail_stop only exists from Airflow 2.7
                since = ".7+" if new_key == "fail_stop" else ""
                console.print(
                    f"[yellow]{dag_name}: {key} is {new_key} in Airflow "
                    f"{self.airflow_version}{since}, renamed[/yellow]"
                )
            adapted[new_key] = value
        if self.airflow_version == 3:
            self._warn_airflow_3_behavior_changes(dag_name, adapted)
        return adapted

    def _warn_airflow_3_behavior_changes(self, dag_name: str, dag_args: Dict[str, Any]):
        """
        Warn about DAG settings Airflow 3 still accepts but treats differently, so
        the DAG loads yet doesn't behave as it did on Airflow 2.
        """
        if "schedule" not in dag_args:
            console.print(
                f"[yellow]{dag_name}: no schedule set. Airflow 3 defaults to none, so "
                f"the DAG only runs when triggered (Airflow 2 defaulted to daily)."
                f"[/yellow]"
            )
        notifications = dag_args.get("notifications")
        callbacks = set(dag_args)
        if isinstance(notifications, dict):  # anything else fails later, explained
            callbacks |= set(notifications)
        if "sla_miss_callback" in callbacks:
            console.print(
                f"[yellow]{dag_name}: sla_miss_callback has no effect in Airflow 3, "
                f"which removed SLAs.[/yellow]"
            )

    def _merge_secret_nodes(self, secret_nodes, yml_dag) -> Dict[str, Any]:
        if isinstance(secret_nodes, dict):
            for node_name, node_config in secret_nodes.get("nodes", {}).items():
                yml_node = yml_dag.get("nodes", {}).get(node_name)
                if yml_node:
                    yml_dag["nodes"][node_name] = deep_merge(node_config, yml_node)
        elif isinstance(secret_nodes, list):  # Datacoves secrets
            replace_secrets(secret_nodes, yml_dag)
        return yml_dag

    def _discover_secrets(self, yml_dag: Dict[str, Any]):
        """
        Load secrets locally/remotely, and merge their 'nodes' into YML file ones
        """
        if self.secrets_path:
            for secret in glob(f"{self.secrets_path}/*.yml"):
                secret_data = yaml.full_load(open(secret))
                yml_dag = self._merge_secret_nodes(secret_data, yml_dag)

        if self.secrets_manager and contains_secret(yml_dag):
            yml_dag = self._merge_secret_nodes(self._get_secret_manager_data(), yml_dag)

        return yml_dag

    def _get_secret_manager_data(self):
        """
        Retrieve the secrets manager's data, once per run and only when a DAG asks
        for it: a run must not need the manager's credentials to generate DAGs that
        hold no `secret()` reference
        """
        if self.secret_data_error:
            # The manager's settings are the run's, not this DAG's: report them
            # once and skip every DAG that needs them, rather than repeating
            raise GenerateAirflowDagsException(
                "Skipped: the secrets manager is unavailable, as reported above"
            )
        if self.secret_data is None:
            try:
                self.secret_data = load_secret_manager_data(self)
            except Exception as e:
                # Reaching the manager fails for reasons well beyond its settings
                # -- a revoked token, an unreachable host -- and none of those are
                # this DAG's to fix either
                self.secret_data_error = e
                # dbt-coves' own messages carry markup, anything else is raw text
                detail = str(e) if isinstance(e, DbtCovesException) else escape(str(e))
                raise GenerateAirflowDagsException(
                    f"Could not read the secrets manager: {detail}"
                ) from e
        return self.secret_data

    def generate_node(self, node_name: str, node_conf: Dict[str, Any]):
        """
        Node generation entrypoint
        """
        try:
            node_type = node_conf.pop("type")
        except KeyError:
            raise GenerateAirflowDagsException(
                f"Node [red][b][i]{node_name}[/i][/b][/red] has no [i]'task'[/i] or "
                f"[i]'task_group'[/i] type"
            )
        if node_type == "task_group":
            self.generate_task_group(node_name, node_conf)
        if node_type == "task":
            task_output = self.generate_task_output(node_name, node_conf)
            self.dag_output["dag"].extend(task_output)

    def get_generator_class(self, generator: str):
        """
        Import Generator from `generators_folder` CLI flag
        Default value is dbt-coves-provided `airflow_generators` module
        """
        module = importlib.import_module(
            self.get_config_value("generators_folder").replace("/", ".")
        )
        return getattr(module, generator)

    def _merge_generator_configs(
        self, tg_conf: Dict[str, Any], generator: str
    ) -> Dict[str, Any]:
        """
        Merge the generator configs between YML Dag and dbt-coves `generators_params` config
        """
        generators_params = self.get_config_value("generators_params")
        coves_config_generators_params = generators_params.get(generator, {})
        if self.secrets_manager and contains_secret(coves_config_generators_params):
            replace_secrets(
                self._get_secret_manager_data(), coves_config_generators_params
            )
        return deep_merge(tg_conf, coves_config_generators_params)

    def generate_task_group(self, tg_name: str, tg_conf: Dict[str, Any]):
        """
        Generate Task Groups, using YML's `generator` or `tasks`
        """
        self.dag_output["imports"].append(
            f"from {self._decorators_module} import task_group\n"
        )
        tg_tooltip = tg_conf.pop("tooltip", "")
        task_group_output = [
            f"{' ' * 4}@task_group(group_id='{tg_name}', tooltip='{tg_tooltip}',)\n",
            f"{' ' * 4}def {tg_name}():\n",
        ]
        generator = tg_conf.pop("generator", "")
        tasks = tg_conf.pop("tasks", {})

        if generator:
            generator_class = self.get_generator_class(generator)
            tg_conf = self._merge_generator_configs(tg_conf, generator)
            generator_instance = generator_class(**tg_conf)
            for operator in generator_instance.imports:
                self._add_operator_import_to_output(operator)
            tasks = generator_instance.generate_tasks()

            for task_call in tasks.values():
                if type(task_call) is str:
                    task_group_output.append(f"{' ' * 8}{task_call}\n")
                elif isinstance(task_call, dict):
                    trigger = task_call.get("trigger", {})
                    sensor = task_call.get("sensor", {})
                    task_group_output.append(f"{' ' * 8}{trigger.get('call', '')}\n")
                    task_group_output.append(f"{' ' * 8}{sensor.get('call', '')}\n")
                    if sensor:
                        task_group_output.append(
                            f"{' ' * 8}{trigger['name']} >> {sensor['name']}\n"
                        )
                    else:
                        task_group_output.append(f"{' ' * 8}{trigger['name']}\n")

        elif tasks:
            for name, conf in tasks.items():
                output = self.generate_task_output(name, conf, is_task_taskgroup=True)
                task_group_output.extend(output)

        if len(task_group_output) == 2:
            # No tasks were added — emit pass to keep the function body valid
            task_group_output.append(
                f"{' ' * 8}pass # XXX dbt-coves did not receive a task here\n"
            )

        tg_variable_name = f"tg_{tg_name}"
        task_group_output.append(f"{' ' * 4}{tg_variable_name} = {tg_name}()\n")
        self.generated_groups[tg_name] = tg_variable_name
        self.dag_output["dag"].extend(task_group_output)

    def _add_operator_import_to_output(self, operator: str):
        """
        Dump Operator's full name into `from {module} import {class}`
        If `validate_operators` was passed, it will be imported at runtime
        """
        operator_parts = operator.split(".")
        module = f"{'.'.join(operator_parts[:-1])}"
        _class = operator_parts[-1]
        if self.validate_operators:
            try:
                getattr(importlib.import_module(module), _class)
            except (ImportError, AttributeError, ValueError):
                raise GenerateAirflowDagsException(
                    f"Can't import operator {_class} from module {module}"
                )
        self._check_airflow_version_compat(operator)
        self.dag_output["imports"].append(f"from {module} import {_class}\n")

    def _check_airflow_version_compat(self, reference: str):
        """
        Check an operator path or a custom import statement against the target
        Airflow version: airflow.sdk doesn't exist in Airflow 2 (an error, which
        skips the DAG), and Airflow 3 deprecates airflow.decorators and the
        airflow.operators / airflow.sensors modules (a warning, once per
        reference).
        """
        module = _referenced_module(reference)
        if self.airflow_version == 2 and _in_module(module, "airflow.sdk"):
            raise GenerateAirflowDagsException(
                f"{escape(reference)} needs Airflow 3: airflow.sdk doesn't exist in "
                f"Airflow 2 (generate with --airflow-version 3, or use the Airflow 2 "
                f"import)"
            )
        if self.airflow_version != 3 or reference in self.warned_airflow_3_moved:
            return
        if any(_in_module(module, moved) for moved in AIRFLOW_3_MOVED_MODULES):
            reason = (
                "core operators and sensors moved to the "
                "apache-airflow-providers-standard package (e.g. "
                "airflow.providers.standard.operators.bash.BashOperator)"
            )
        elif _in_module(module, "airflow.decorators"):
            reason = "use airflow.sdk, which dbt-coves imports the DAG decorators from"
        else:
            return
        self.warned_airflow_3_moved.add(reference)
        console.print(
            f"[yellow]{escape(reference)} is deprecated in Airflow 3: {reason}[/yellow]"
        )

    def generate_airflow_k8s_inner_conf(self, task_name: str, config: Dict[str, Any]):
        """
        Generate the multiline config section of Airflow's K8S_INNER_CONF template
        """
        config_lines = ""
        k8s_resources_string_template = (
            "resources=k8s.V1ResourceRequirements(requests={resources}),\n"
        )
        for key, value in config.items():
            if key == "resources":
                config_lines += k8s_resources_string_template.format(resources=value)
            else:
                config_lines += f"{key}= '{value}',\n"
        return config_lines

    def create_and_append_k8s_config(self, task_name: str, task_conf: Dict[str, Any]):
        """
        Create config section of AIRFLOW_K8S_CONFIG template
        Extend template into Globals section of the Python file
        Update task_conf with new `executor_config=config` task arguments
        """
        config_global_name = f"{task_name.upper()}_CONFIG"
        inner_config_lines = self.generate_airflow_k8s_inner_conf(
            task_name, task_conf.pop("config")
        )
        self.dag_output["globals"].append(
            f"{config_global_name}="
            f"{AIRFLOW_K8S_CONFIG_TEMPLATE.format(config=inner_config_lines)}\n"
        )
        self.dag_output["imports"].append(
            "from kubernetes.client import models as k8s\n"
        )
        # The name of the global just appended, not a string to quote
        task_conf["executor_config"] = RawExpr(config_global_name)

    def generate_task_output(
        self, task_name: str, task_conf: Dict[str, Any], is_task_taskgroup=False
    ):
        """
        Generate output for `tasks`: they can be individual (decorated with @type)
        or part of a task-group
        """
        if "config" in task_conf:
            self.create_and_append_k8s_config(task_name, task_conf)
        indent = 8 if is_task_taskgroup else 4
        task_decorator = task_conf.pop("task_decorator", None)
        if task_decorator:
            # Parse task_decorator and arguments
            self.dag_output["imports"].append(
                f"from {self._decorators_module} import task\n"
            )
            bash_command = task_conf.pop("bash_command", "")
            dependencies = task_conf.pop("dependencies", [])

            # Extract additional arguments for the decorator
            decorator_args = []
            for key, value in task_conf.items():
                if isinstance(value, RawExpr):  # `!py` tagged: emit as written
                    value = str(value)
                elif isinstance(
                    value, dict
                ):  # Handle nested dictionaries (e.g., overrides)
                    self._register_datetime_import(value)
                    value = f"{value}"  # Render as a Python dictionary
                elif isinstance(value, str):
                    self._warn_on_quoted_py_tag(key, value)
                    value = f'"{value}"'
                else:
                    value = self._python_value(value)
                decorator_args.append(f"{key}={value}")

            # Render decorated function, leaving out the argument line entirely
            # when the task has no arguments -- a lone `,` doesn't parse
            task_output = [
                f"{' ' * indent}@task.{task_decorator}(\n",
                *(
                    [f"{' ' * (indent + 4)}{', '.join(decorator_args)},\n"]
                    if decorator_args
                    else []
                ),
                f"{' ' * indent})\n",
                f"{' ' * indent}def {task_name}():\n",
                f'{" " * (indent + 4)}return "{bash_command}"\n',
                f"{' ' * (indent)}{task_name} = {task_name}()\n",
            ]
        else:
            try:
                operator = task_conf.pop("operator")
            except KeyError:
                raise GenerateAirflowDagsException(
                    f"Task [red][b][i]{task_name}[/i][/b][/red] has no [i]'operator'[/i]"
                )
            dependencies = task_conf.pop("dependencies", [])
            task_output = []
            task_output.extend(
                [
                    f"{' ' * indent}{task_name} = {operator.split('.')[-1]}(\n",
                    f"{' ' * indent}task_id='{task_name}',\n",
                    f"{' ' * indent}{self.dag_args_to_string(task_conf)}\n",
                    f"{' ' * indent})\n",
                ]
            )
            upstream_list = [self.generated_groups.get(d, d) for d in dependencies]
            self._add_operator_import_to_output(operator)
        if dependencies:
            upstream_list = [self.generated_groups.get(d, d) for d in dependencies]
            if is_task_taskgroup:
                task_output.append(
                    f"{' ' * indent}[{', '.join(upstream_list)}] >> {task_name} \n"
                )
            else:
                self.collected_dependencies.append((upstream_list, task_name))
        return task_output
