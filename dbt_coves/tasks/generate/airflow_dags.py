import datetime
import importlib
from glob import glob
from pathlib import Path
from typing import Any, Dict

import yaml
from black import FileMode, format_str
from rich.console import Console

from dbt_coves.tasks.base import NonDbtBaseConfiguredTask
from dbt_coves.utils.secrets import load_secret_manager_data
from dbt_coves.utils.tracking import trackable
from dbt_coves.utils.yaml import deep_merge

console = Console()


class GenerateAirflowDagsException(Exception):
    pass


class GenerateAirflowDagsTask(NonDbtBaseConfiguredTask):
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # Custom constructor to convert to datetime.datetime
    def date_constructor(self, loader, node):
        value = loader.construct_scalar(node)
        return datetime.datetime.strptime(value, "%Y-%m-%d")

    def get_config_value(self, key):
        return self.coves_config.integrated["generate"]["airflow_dags"][key]

    def _generate_dag(self, yml_filepath: Path):
        yaml.FullLoader.add_constructor("tag:yaml.org,2002:timestamp", self.date_constructor)
        if self.dags_path:
            dag_destination = self.dags_path.joinpath(f"{yml_filepath.stem}.py")
        else:
            dag_destination = yml_filepath.with_suffix(".py")
        self.build_dag_file(
            destination_path=dag_destination,
            dag_name=yml_filepath.stem,
            yml_dag=yaml.full_load(open(yml_filepath)),
        )

    def _inform_results(self):
        if len(self.generation_results) > 0:
            console.print(
                "[green]:heavy_check_mark:[/green] Generated Airflow DAGs: "
                f"[green]{', '.join(self.generation_results)}[/green]"
            )

    @trackable
    def run(self):
        self.generation_results = set()
        self.ymls_path = Path(self.get_config_value("yml_path"))
        self.validate_operators = self.get_config_value("validate_operators")
        self.secrets_path = self.get_config_value("secrets_path")
        self.secrets_manager = self.get_config_value("secrets_manager")
        self.generated_groups = {}
        if self.secrets_path and self.secrets_manager:
            raise GenerateAirflowDagsException(
                "Can't use 'secrets_path' and 'secrets_manager' simultaneously."
            )
        self.dags_path = self.get_config_value("dags_path")
        if self.dags_path:
            self.dags_path = Path(self.dags_path).resolve()
            self.dags_path.mkdir(exist_ok=True, parents=True)
        if self.ymls_path.is_dir():
            for yml_filepath in glob(f"{self.ymls_path}/*.yml"):
                self._generate_dag(Path(yml_filepath))
        else:
            self._generate_dag(self.ymls_path)

        self._inform_results()
        return 0

    def dag_args_to_string(self, yaml, indent=2):
        """
        Converts a dictionary to a string of arguments for the DAG constructor.
        """
        dag_args = ""
        for key, value in yaml.items():
            dag_value = f'"{value}"' if isinstance(value, str) else value
            dag_args += f'{indent * " "}{key}={dag_value},\n'
        return dag_args[:-2]

    def build_dag_file(self, destination_path: Path, dag_name: str, yml_dag: Dict[str, Any]):
        """
        Generate DAG Python file based on YML configuration
        """
        yml_dag = self._discover_secrets(yml_dag)
        nodes = yml_dag.pop("nodes", {})
        default_args = {"default_args": yml_dag.pop("default_args", {})}
        self.dag_output = {
            "imports": [
                "from airflow.decorators import dag, task_group\n",
                "import datetime\n",
            ],
            "dag": [
                "@dag(\n",
                f"{self.dag_args_to_string(default_args)},\n",
                f"{self.dag_args_to_string(yml_dag)}\n",
                ")\n",
                f"def {dag_name}():\n",
            ],
        }
        first_node = None
        for node_name, node_conf in nodes.items():
            if not first_node:
                first_node = node_name
            self.generate_node(node_name, node_conf)

        self.dag_output["dag"].append(
            f"{' '*4}{self.generated_groups.get(first_node, first_node)}\n"
        )
        self.dag_output["dag"].append(f"dag = {dag_name}()\n")

        with open(destination_path, "w") as f:
            final_output = "".join(set(self.dag_output["imports"])) + "".join(
                self.dag_output["dag"]
            )
            black_formatted = format_str(final_output, mode=FileMode())

            f.write(black_formatted)
            self.generation_results.add(dag_name)

    def _discover_secrets(self, yml_dag: Dict[str, Any]):
        """
        Load secrets locally/remotely, and merge their 'nodes' into YML file ones
        """
        if self.secrets_path:
            for secret in glob(f"{self.secrets_path}/*.yml"):
                secret_data = yaml.full_load(open(secret))
                yml_dag = deep_merge(secret_data, yml_dag)

        if self.secrets_manager:
            secret_data = load_secret_manager_data(self)
            for secret in secret_data:
                yml_dag = deep_merge(secret.get("value", {}), yml_dag)

        return yml_dag

    def generate_node(self, node_name: str, node_conf: Dict[str, Any]):
        """
        Node generation entrypoint
        """
        node_type = node_conf.pop("type", "task")
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
        For example.
        generators_params:
            AirbyteDbtGenerator:
                host: "http://localhost"
                port: 8000
        """
        generators_params = self.get_config_value("generators_params")
        coves_config_generators_params = generators_params.get(generator, {})
        return deep_merge(tg_conf, coves_config_generators_params)

    def generate_task_group(self, tg_name: str, tg_conf: Dict[str, Any]):
        """
        Generate Task Groups, using YML's `generator` or `tasks`
        """
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
                task_group_output.append(f"{' '*8}{task_call}\n")

            if len(tasks) > 1:
                task_group_output.append(f"{' ' *8}{' >> '.join(tasks.keys())}\n")
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

    def generate_task_output(
        self, task_name: str, task_conf: Dict[str, Any], is_task_taskgroup=False
    ):
        """
        Generate output for `tasks`: they can be individual (decorated with @type)
        or part of a task-group
        """
        indent = 8 if is_task_taskgroup else 4
        operator = task_conf.pop("operator")
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
