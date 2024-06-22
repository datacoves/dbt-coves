## dbt-coves generate airflow-dags

```console
dbt-coves generate airflow-dags
```

Translate YML files into their Airflow Python code equivalent. With this, DAGs can be easily written with some `key:value` pairs.

The basic structure of these YMLs must consist of:

- Global configurations (description, schedule_interval, tags, catchup, etc.)
- `default_args`
- `nodes`: where tasks and task groups are defined
  - each Node is a nested object, with it's `name` as key and it's configuration as values.
    - this configuration must cover:
      - `type`: 'task' or 'task_group'
      - `operator`: Airflow operator that will run the tasks (full _module.class_ naming)
      - `dependencies`: whether the task is dependent on another one(s)
      - any `key:value` pair of [Operator arguments](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/index.html)

### Airflow DAG Generators

When a YML Dag `node` is of type `task_group`, **Generators** can be used instead of `Operators`.

Generators are custom classes that receive YML `key:value` pairs and return one or more tasks for the respective task group. Any pair specified other than `type: task_group` will be passed to the specified `generator`, and it has the responsibility of returning N amount of `task_name = Operator(params)`.

We provide some prebuilt Generators:

- `AirbyteGenerator` creates `AirbyteTriggerSyncOperator` tasks (one per Airbyte connection)
  - It must receive Airbyte's `host` and `port`, `airbyte_conn_id` (Airbyte's connection name on Airflow) and a `connection_ids` list of Airbyte Connections to Sync
- `FivetranGenerator`: creates `FivetranOperator` tasks (one per Fivetran connection)
  - It must receive Fivetran's `api_key`, `api_secret` and a `connection_ids` list of Fivetran Connectors to Sync.
- `AirbyteDbtGenerator` and `FivetranDbtGenerator`: instead of passing them Airbyte or Fivetran connections, they use dbt to discover those IDs. Apart from their parent Generators mandatory fields, they can receive:
  - `dbt_project_path`: dbt/project/folder
  - `virtualenv_path`: path to a virtualenv in case dbt within a specific virtual env
  - `run_dbt_compile`: true/false always run the dbt compile command
  - `run_dbt_deps`: true/false always run the dbt deps command

### Basic YML DAG example:

```yaml
description: "dbt-coves DAG"
schedule_interval: "@hourly"
tags:
  - version_01
default_args:
  start_date: 2023-01-01
catchup: false
nodes:
  airbyte_dbt:
    type: task_group
    tooltip: "Sync dbt-related Airbyte connections"
    generator: AirbyteDbtGenerator
    host: http://localhost
    port: 8000
    dbt_project_path: /path/to/dbt_project
    virtualenv_path: /virtualenvs/dbt_160
    run_dbt_compile: false
    run_dbt_deps: false
    airbyte_conn_id: airbyte_connection
  task_1:
    operator: airflow.operators.bash.DatacovesBashOperator
    bash_command: "echo 'This runs after airbyte tasks'"
    dependencies: ["airbyte_dbt"]
```

### Create your custom Generator

You can create your own DAG Generator. Any `key:value` specified in the YML DAG will be passed to it's constructor.

This Generator needs:

- a `imports` attribute: a list of _module.class_ Operator of the tasks it outputs
- a `generate_tasks` method that returns the set of `"task_name = Operator()"` strings to write as the task group tasks.

```python
class PostgresGenerator():
    def __init__(self) -> None:
        """ Any key:value pair in the YML Dag will get here """
        self.imports = ["airflow.providers.postgres.operators.postgres.PostgresOperator"]

    def generate_tasks(self):
        """ Use your custom logic and return N `name = PostgresOperator()` strings """
        raise NotImplementedError
```

### Arguments

`dbt-coves generate airflow-dags` supports the following args:

```console
--yml-path --yaml-path
# Path to the folder containing YML files to translate into Python DAGs

--dag-path
# Path to the folder where Python DAGs will be generated.

--validate-operators
# Ensure Airflow operators are installed by trying to import them before writing to Python.
# Flag: no value required

--generators-folder
# Path to your Python module with custom Generators

--generators-params
# Object with default values for the desired Generator(s)
# For example: {"AirbyteGenerator": {"host": "http://localhost", "port": "8000"}}

--secrets-path
# Secret files location for DAG configuration, i.e. 'yml_path/secrets/'
# Secret content must match the YML dag spec of `nodes -> node_name -> config`
```
