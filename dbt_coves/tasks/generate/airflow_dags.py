import datetime
import importlib
import textwrap
from glob import glob
from pathlib import Path
from typing import Any, Dict

import isort
import yaml
from black import FileMode, format_str
from rich.console import Console

from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils.secrets import load_secret_manager_data
from dbt_coves.utils.tracking import trackable
from dbt_coves.utils.yaml import deep_merge

console = Console()

AIRFLOW_K8S_CONFIG_TEMPLATE = textwrap.dedent(
    """{{
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        {config}
                    )
                ]
            )
        ),
}}"""
)


class GenerateAirflowDagsException(Exception):
    pass


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
            "--yml-path",
            "--yaml-path",
            type=str,
            required=False,
            help="Folder where YML files will be read from",
        )
        subparser.add_argument(
            "--dags-path",
            type=str,
            required=False,
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
            help="Object with default values for the desired Generator(s), i.e {'AirbyteDbtGenerator' "
            "{'host': 'http://localhost'}}",
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
        subparser.add_argument("--secrets-url", type=str, help="Secret credentials provider url")
        subparser.add_argument(
            "--secrets-token", type=str, help="Secret credentials provider token"
        )
        subparser.add_argument("--secrets-project", type=str, help="Secret credentials project")
        subparser.add_argument("--secrets-tags", type=str, help="Secret credentials tags")
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

    def get_config_value(self, key):
        return self.coves_config.integrated["generate"]["airflow_dags"][key]

    def _generate_dag(self, yml_filepath: Path):
        yaml.FullLoader.add_constructor("tag:yaml.org,2002:timestamp", self.date_constructor)
        console.print(f"Generating [b][i]{yml_filepath.stem}[/i][/b]")
        try:
            if self.dags_path:
                dag_destination = self.dags_path.joinpath(f"{yml_filepath.stem}.py")
            else:
                dag_destination = yml_filepath.with_suffix(".py")
            self.build_dag_file(
                destination_path=dag_destination,
                dag_name=yml_filepath.stem,
                yml_dag=yaml.full_load(open(yml_filepath)),
            )
        except GenerateAirflowDagsException as e:
            console.print(f"[red]{e}[/red]")

    @trackable
    def run(self):
        ymls_path = self.get_config_value("yml_path")
        dags_path = self.get_config_value("dags_path")
        if not (ymls_path and dags_path):
            raise GenerateAirflowDagsException(
                "You must provide source ([i]--yml-path[/i]) and destination ([i]--dags-path[/i]) of DAGs"
            )
        self.validate_operators = self.get_config_value("validate_operators")
        self.secrets_path = self.get_config_value("secrets_path")
        self.secrets_manager = self.get_config_value("secrets_manager")
        self.generated_groups = {}
        if self.secrets_path and self.secrets_manager:
            raise GenerateAirflowDagsException(
                "Can't use 'secrets_path' and 'secrets_manager' simultaneously."
            )
        self.ymls_path = Path(ymls_path).resolve()
        self.dags_path = Path(dags_path).resolve()
        self.dags_path.mkdir(exist_ok=True, parents=True)
        if self.ymls_path.is_dir():
            for yml_filepath in glob(f"{self.ymls_path}/*.yml"):
                self._generate_dag(Path(yml_filepath))
        else:
            self._generate_dag(self.ymls_path)
        return 0

    def dag_args_to_string(self, yaml, indent=2):
        """
        Converts a dictionary to a string of arguments for the DAG constructor.
        """
        dag_args = ""
        for key, value in yaml.items():
            if "custom_callbacks" in key:
                for call in self.generate_custom_callbacks(yaml["custom_callbacks"]):
                    dag_args += f"{indent * ' '}{call},\n"
            else:
                dag_value = (
                    f'"{value}"' if (isinstance(value, str) and "config" not in key) else value
                )
                dag_args += f'{indent * " "}{key}={dag_value},\n'
        return dag_args[:-2]

    def generate_custom_callbacks(self, custom_callbacks: Dict[str, Any]):
        """
        Generate imports, globals, and return DAG `callback=run_callback` settings
        """
        callback_calls = []
        for callback, definition in custom_callbacks.items():
            module = definition["module"]
            callable = definition["callable"]
            custom_callback_name = f"run_{callable}"
            args = definition.get("args", {})
            self.dag_output["imports"].append(f"from {module} import {callable}\n")
            self.dag_output["globals"].extend(
                [
                    f"def {custom_callback_name}(context):\n",
                    f"{2*' '}{callable}(context, {self.dag_args_to_string(args)})\n",
                ]
            )
            callback_calls.append(f"{2 * ' '}{callback}={custom_callback_name}")
        return callback_calls

    def build_dag_file(self, destination_path: Path, dag_name: str, yml_dag: Dict[str, Any]):
        """
        Generate DAG Python file based on YML configuration
        """
        yml_dag = self._discover_secrets(yml_dag)
        try:
            nodes = yml_dag.pop("nodes")
        except KeyError:
            raise GenerateAirflowDagsException(
                f"YML file [red][b][i]{dag_name}[/i][/b][/red] must contain a 'nodes' section"
            )
        default_args = {"default_args": yml_dag.pop("default_args", {})}
        self.dag_output = {
            "imports": [
                "from airflow.decorators import dag\n",
                "import datetime\n",
            ],
            "globals": [],
            "dag": [
                "@dag(\n",
                f"{self.dag_args_to_string(default_args)},\n",
            ],
        }
        self.dag_output["dag"].extend(
            [
                f"{self.dag_args_to_string(yml_dag)}\n",
                ")\n",
                f"def {dag_name}():\n",
            ]
        )
        for node_name, node_conf in nodes.items():
            self.generate_node(node_name, node_conf)

        self.dag_output["dag"].append(f"dag = {dag_name}()\n")

        with open(destination_path, "w") as f:
            final_output = (
                "".join(set(self.dag_output["imports"]))
                + "".join(self.dag_output["globals"])
                + "".join(self.dag_output["dag"])
            )
            try:
                black_formatted = format_str(final_output, mode=FileMode())
                isort_formatted = isort.code(black_formatted)
                f.write(isort_formatted)
            except Exception as exc:
                f.write(final_output)
                console.print(f"DAG {dag_name} resulted in an invalid DAG, skipping. Error: {exc}")

    def _merge_secret_nodes(self, secret_nodes, yml_dag) -> Dict[str, Any]:
        for node_name, node_config in secret_nodes.get("nodes", {}).items():
            yml_node = yml_dag.get("nodes", {}).get(node_name)
            if yml_node:
                yml_dag["nodes"][node_name] = deep_merge(node_config, yml_node)
        return yml_dag

    def _discover_secrets(self, yml_dag: Dict[str, Any]):
        """
        Load secrets locally/remotely, and merge their 'nodes' into YML file ones
        """
        if self.secrets_path:
            for secret in glob(f"{self.secrets_path}/*.yml"):
                secret_data = yaml.full_load(open(secret))
                yml_dag = self._merge_secret_nodes(secret_data, yml_dag)

        if self.secrets_manager:
            secret_data = load_secret_manager_data(self)
            for secret in secret_data:
                yml_dag = self._merge_secret_nodes(secret.get("value", {}), yml_dag)

        return yml_dag

    def generate_node(self, node_name: str, node_conf: Dict[str, Any]):
        """
        Node generation entrypoint
        """
        try:
            node_type = node_conf.pop("type")
        except KeyError:
            raise GenerateAirflowDagsException(
                f"Node [red][b][i]{node_name}[/i][/b][/red] has no [i]'task'[/i] or [i]'task_group'[/i] type"
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

    def _merge_generator_configs(self, tg_conf: Dict[str, Any], generator: str) -> Dict[str, Any]:
        """
        Merge the generator configs between YML Dag and dbt-coves `generators_params` config
        """
        generators_params = self.get_config_value("generators_params")
        coves_config_generators_params = generators_params.get(generator, {})
        return deep_merge(tg_conf, coves_config_generators_params)

    def generate_task_group(self, tg_name: str, tg_conf: Dict[str, Any]):
        """
        Generate Task Groups, using YML's `generator` or `tasks`
        """
        self.dag_output["imports"].append("from airflow.decorators import task_group\n"),
        task_group_output = [
            f"{' '*4}@task_group(group_id='{tg_name}', tooltip='{tg_conf.pop('tooltip', '')}')\n",
            f"{' '*4}def {tg_name}():\n",
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
                if type(task_call) == str:
                    task_group_output.append(f"{' '*8}{task_call}\n")
                elif isinstance(task_call, dict):
                    trigger = task_call.pop("trigger")
                    sensor = task_call.pop("sensor")
                    task_group_output.append(f"{' ' *8}{trigger['call']}\n")
                    task_group_output.append(f"{' ' *8}{sensor['call']}\n")
                    task_group_output.append(f"{' ' *8}{trigger['name']} >> {sensor['name']}\n")

        elif tasks:
            for name, conf in tasks.items():
                output = self.generate_task_output(name, conf, is_task_taskgroup=True)
                task_group_output.extend(output)

        tg_variable_name = f"tg_{tg_name}"
        task_group_output.append(f"{' '*4}{tg_variable_name} = {tg_name}()\n")
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
                importlib.import_module(module).instance
            except ImportError:
                raise GenerateAirflowDagsException(
                    f"Can't import operator {_class} from module {module}"
                )

        self.dag_output["imports"].append(f"from {module} import {_class}\n")

    def generate_airflow_k8s_inner_conf(self, task_name: str, config: Dict[str, Any]):
        """
        Generate the multiline config section of Airflow's K8S_INNER_CONF template
        """
        k8s_resources_string_template = (
            "resources=k8s.V1ResourceRequirements(requests={resources}),\n"
        )
        config_lines = f"name='{task_name}',\n "
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
            f"{config_global_name}={AIRFLOW_K8S_CONFIG_TEMPLATE.format(config=inner_config_lines)}\n"
        )
        self.dag_output["imports"].append("from kubernetes.client import models as k8s\n")
        task_conf["executor_config"] = config_global_name

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
                f"{' '*indent}{task_name} = {operator.split('.')[-1]}(\n",
                f"{' '*indent}task_id='{task_name}',\n",
                f"{' '*indent}{self.dag_args_to_string(task_conf)}\n",
                f"{' '*indent})\n",
            ]
        )
        upstream_list = [self.generated_groups.get(d, d) for d in dependencies]
        if dependencies:
            task_output.append(
                f"{' '*indent}{task_name}.set_upstream([{','.join(upstream_list)}])\n"
            )

        self._add_operator_import_to_output(operator)
        return task_output
