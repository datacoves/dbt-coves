## dbt-coves generate airflow-dags

```console
dbt-coves generate airflow-dags --airflow-version 3
```

Translate YML files into their Airflow Python code equivalent. With this, DAGs can be easily written with some `key:value` pairs.

### Airflow version (required)

DAGs generated for Airflow 2 don't work on Airflow 3, and the other way around. For example, Airflow 3 imports `@dag` from `airflow.sdk`, which Airflow 2 doesn't have, and it no longer accepts `schedule_interval`. So you must say which major version the DAGs are for, with `--airflow-version 2|3` or `generate.airflow_dags.airflow_version` in `.dbt_coves/config.yml`. Without it, the command stops before generating anything.

**Datacoves users:** the VS Code task that generates a DAG from the open YML file runs in your dbt project folder, so set `generate.airflow_dags.airflow_version` in the `.dbt_coves/config.yml` of that folder (not the repository root):

```yaml
generate:
  airflow_dags:
    airflow_version: 3 # or 2, matching your environment's Airflow
```

The value can also come from an environment variable you set, e.g. `airflow_version: "{{ env_var('MY_AIRFLOW_VERSION') }}"`.

For Airflow 2, the generated DAGs target Airflow 2.4 and later, where `schedule` replaced `schedule_interval`.

The version decides:

- **Where the decorators come from:** `@dag`, `@task` and `@task_group` are imported from `airflow.decorators` for Airflow 2, and from `airflow.sdk` for Airflow 3.
- **How DAG arguments are adapted.** Arguments the other version renamed are renamed in the generated DAG, with a notice, so the same YML loads on either version:

  | Generating for | Renamed | Not supported (the DAG is skipped with an error) |
  |---|---|---|
  | Airflow 3 | `schedule_interval` and `timetable` → `schedule`, `fail_stop` → `fail_fast`, `concurrency` → `max_active_tasks` | `default_view`, `orientation`, `full_filepath` |
  | Airflow 2 | `fail_fast` → `fail_stop` (Airflow 2.7+) | `deadline`, `allowed_run_types`, `disable_bundle_versioning`, `rerun_with_latest_version`, `task_group` |

  Setting both the old and the new name (e.g. `schedule_interval` and `schedule`) is an error.
- **Warnings for changed behavior on Airflow 3.** Airflow 3 still accepts some settings but treats them differently, so the DAG loads but behaves differently than on Airflow 2. When generating for Airflow 3, dbt-coves warns when:
  - a DAG sets no `schedule`. Airflow 3 defaults to none, so the DAG only runs when triggered; Airflow 2 defaulted to daily.
  - `sla_miss_callback` is set, at the top level or under `notifications`. It has no effect, because Airflow 3 removed SLAs.

  Airflow 3 also changed `catchup` to default to off. Set `catchup` explicitly if a DAG relies on it.
- **Checks on operators and custom `imports`.** A task's `operator` and the YML's `imports` are checked against the version:
  - For Airflow 2, anything from `airflow.sdk` skips the DAG with an error: that module doesn't exist in Airflow 2.
  - For Airflow 3, anything from `airflow.operators` / `airflow.sensors` prints a warning, once per operator path or import line. Those still work, but are deprecated in favor of the `apache-airflow-providers-standard` package (e.g. `airflow.providers.standard.operators.bash.BashOperator`). So does anything from `airflow.decorators`, deprecated in favor of `airflow.sdk`.

Only these DAG-level differences are handled. Task arguments, templates (e.g. `{{ execution_date }}`, removed in Airflow 3) and custom imports are written as given. Airflow's upgrade guide recommends `ruff check --select AIR3` to find the rest.

The basic structure of these YMLs must consist of:

- Global configurations (`description`, `schedule`, `tags`, `catchup`, etc.) - any `key:value` pair here is passed as an argument to Airflow's `@dag(...)` decorator (adapted to the Airflow version, see above).
- `imports`: extra Python import statements the generated file needs (optional)
- `doc_md`: a Markdown string used as the DAG's module docstring and `doc_md` (optional)
- `notifications`: `on_success_callback` / `on_failure_callback` (or any other `@dag()` callback argument) built from a notifier class (optional)
- `default_args`
- `nodes`: where tasks and task groups are defined
  - each Node is a nested object, with it's `name` as key and it's configuration as values.
    - this configuration must cover:
      - `type`: 'task' or 'task_group'
      - `operator`: Airflow operator that will run the task (full _module.class_ naming), **or**
      - `task_decorator`: the name of a `@task.<decorator>` TaskFlow decorator to use instead of an `operator` (e.g. Datacoves' `datacoves_dbt`)
      - `dependencies`: whether the task is dependent on another one(s)
      - any `key:value` pair of [Operator arguments](https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/operators/index.html) (or decorator arguments, when using `task_decorator`)

### Basic YML DAG example:

```yaml
description: "dbt-coves DAG"
schedule: "@hourly"
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
    api_key: "{{ env_var('AIRBYTE_API_KEY') }}"
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

### Custom imports

Use the top-level `imports` key to add any extra import statements the generated DAG needs (for example, to call a helper module from a raw Python expression, see below). Each entry is written verbatim as its own import line; `isort` runs on the generated file, so ordering and de-duplication are handled automatically.

```yaml
imports:
  - "from orchestrate.utils import datacoves_utils"
```

### DAG docstring (`doc_md`)

A `doc_md` block becomes the file's module docstring (rendered by Airflow's UI as the DAG's documentation) and is automatically wired into the `@dag()` call as `doc_md=__doc__`.

```yaml
doc_md: |
  ## Sample DAG showing how to run dbt
  This DAG shows how to run a dbt task
```

### Raw Python expressions with `!py`

Every YML value is rendered into the generated Python as a literal: strings are quoted, numbers/booleans/lists are dumped as-is. Tag a string with `!py` when you need it emitted as an **unquoted Python expression** instead - for example, to call a helper function like Datacoves' `datacoves_utils.set_schedule(...)` or `datacoves_utils.set_default_args(...)`:

```yaml
schedule: !py datacoves_utils.set_schedule("0 0 1 */12 *")
default_args: !py datacoves_utils.set_default_args(owner="Noel Gomez", owner_email="noel@example.com")
```

`!py` can be used anywhere a `key: value` pair is rendered - at the DAG level, inside `default_args`, or as a task/decorator argument. Remember to add the corresponding `imports` entry for any module referenced in the expression.

Putting it all together, this YAML:

```yaml
imports:
  - "from orchestrate.utils import datacoves_utils"
doc_md: |
  ## Sample DAG showing how to run dbt
  This DAG shows how to run a dbt task
schedule: !py datacoves_utils.set_schedule("0 0 1 */12 *")
default_args: !py datacoves_utils.set_default_args(owner="Noel Gomez", owner_email="noel@example.com")
description: "Sample DAG demonstrating how to run dbt in airflow"
tags:
  - transform
catchup: false
nodes:
  run_dbt:
    type: task
    task_decorator: datacoves_dbt
    connection_id: main_key_pair
    bash_command: "dbt debug"
```

generates:

```python
"""
## Sample DAG showing how to run dbt
This DAG shows how to run a dbt task
"""

import datetime

from airflow.decorators import dag, task  # from airflow.sdk with --airflow-version 3
from orchestrate.utils import datacoves_utils


@dag(
    default_args=datacoves_utils.set_default_args(
        owner="Noel Gomez", owner_email="noel@example.com"
    ),
    schedule=datacoves_utils.set_schedule("0 0 1 */12 *"),
    description="Sample DAG demonstrating how to run dbt in airflow",
    tags=["transform"],
    catchup=False,
    doc_md=__doc__,
)
def dbt_dag():
    @task.datacoves_dbt(
        connection_id="main_key_pair",
    )
    def run_dbt():
        return "dbt debug"

    run_dbt = run_dbt()


dag = dbt_dag()
```

### Tasks via `operator` vs `task_decorator`

Most tasks are written with a classic `operator`, which is instantiated directly:

```yaml
nodes:
  transform:
    type: task
    operator: airflow.operators.bash.BashOperator
    bash_command: "dbt run"
```

Alternatively, use `task_decorator` to generate an Airflow [TaskFlow](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/taskflow.html)-style `@task.<decorator>` function instead. This is how Datacoves' custom `datacoves_dbt` (and similar) task decorators are used:

```yaml
nodes:
  run_dbt:
    type: task
    task_decorator: datacoves_dbt
    connection_id: main_key_pair
    bash_command: "dbt debug"
```

Any remaining `key: value` pairs (besides `bash_command` and `dependencies`) become arguments to the decorator itself, e.g. `@task.datacoves_dbt(connection_id="main_key_pair")`.

### Kubernetes executor config

Add a `config` block to a task to generate a `KubernetesExecutor` pod override (`executor_config=...`), including a dedicated `from kubernetes.client import models as k8s` import and a `<TASK_NAME>_CONFIG` global:

```yaml
nodes:
  transform:
    type: task
    operator: airflow.operators.bash.BashOperator
    bash_command: "dbt run"
    config:
      image: datacoves/airflow-pandas:latest
      resources:
        memory: 8Gi
        cpu: 1000m
```

### Notifications / callbacks

Use the top-level `notifications` key to wire up `on_success_callback` / `on_failure_callback` (or any other `@dag()` callback argument) to a notifier class, without having to hand-write the import and instantiation:

```yaml
notifications:
  on_failure_callback:
    notifier: dbt_coves.notifications.slack.SlackNotifier # or `callback`
    args:
      webhook_url: "{{ env_var('SLACK_WEBHOOK_URL') }}"
```

`notifier` (or its alias `callback`) must be the full _module.class_ path of the notifier/callback to import and call; `args` is passed to it as `key=value` arguments (or positional values, when given as a list).

### Airflow DAG Generators

When a YML Dag `node` is of type `task_group`, **Generators** can be used instead of `Operators`.

Generators are custom classes that receive YML `key:value` pairs and return one or more tasks for the respective task group. Any pair specified other than `type: task_group` will be passed to the specified `generator`, and it has the responsibility of returning N amount of `task_name = Operator(params)`.

We provide some prebuilt Generators:

- `AirbyteGenerator` creates `AirbyteTriggerSyncOperator` tasks (one per Airbyte connection)
  - It must receive Airbyte's `host`, `airbyte_conn_id` (Airbyte's connection name on Airflow) and a `connection_ids` list of Airbyte Connections to Sync. `port` and `api_key` are optional (API key required for Airbyte Cloud and modern self-hosted instances)
- `FivetranGenerator`: creates `FivetranOperator` tasks (one per Fivetran connection)
  - It must receive Fivetran's `api_key`, `api_secret` and a `connection_ids` list of Fivetran Connectors to Sync.
- `AirbyteDbtGenerator` and `FivetranDbtGenerator`: instead of passing them Airbyte or Fivetran connections, they use dbt to discover those IDs. Apart from their parent Generators mandatory fields, they can receive:
  - `dbt_project_path`: dbt/project/folder
  - `virtualenv_path`: path to a virtualenv in case dbt within a specific virtual env
  - `run_dbt_compile`: true/false always run the dbt compile command
  - `run_dbt_deps`: true/false always run the dbt deps command
  - `dbt_list_args`: extra arguments for the dbt commands it runs (`dbt ls`, and `dbt compile` / `dbt deps` when enabled), e.g. `"--select tag:daily_run_airbyte"`

  These Generators run `dbt ls --resource-type source` in `dbt_project_path` and read the `manifest.json` it writes, from the same place dbt does: the project folder (`--project-dir` in `dbt_list_args`, or `DBT_PROJECT_DIR`) joined with the target path (`--target-path` in `dbt_list_args`, then `DBT_TARGET_PATH`, then `target-path` in `dbt_project.yml`, then `target`). If `target-path` in `dbt_project.yml` uses Jinja, which dbt-coves can't render, it warns and assumes `target`; pass `--target-path` in `dbt_list_args` if dbt writes elsewhere. dbt needs a working profile to run, so if `dbt ls` fails (for example, `profiles.yml` isn't found), the DAG is skipped with the command and dbt's output.

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

### Secrets

Sensitive values (API keys, connection credentials, etc.) don't need to be hardcoded in the YML DAGs. Two mechanisms are supported, and they merge into the `nodes` section of the YML DAG before it's translated to Python:

- **Local secret files** (`--secrets-path`): YML files placed in a folder, each with the same `nodes -> node_name -> ...` shape as the DAG YML, deep-merged into it.
- **Secrets manager** (`--secrets-manager`): currently supports `datacoves`. Requires `--secrets-url`, `--secrets-token` and `--secrets-environment` (or the `DATACOVES__SECRETS_URL`, `DATACOVES__SECRETS_TOKEN` and `DATACOVES__ENVIRONMENT_SLUG` env vars). Inside a Datacoves environment (code-server, Airflow) these three env vars are set automatically, so the flags/env vars typically only need to be passed explicitly outside of it. Optionally filter the secrets fetched with `--secrets-tags` and/or `--secrets-key`. Reference a fetched secret anywhere in the YML DAG with `{{ secret('secret_slug') }}`.

`--secrets-path` and `--secrets-manager` are mutually exclusive.

### Arguments

`dbt-coves generate airflow-dags` supports the following args:

```console
--airflow-version
# Required: major version of Airflow the DAGs are for, 2 or 3

--yml-path --yaml-path
# Path to the folder containing YML files to translate into Python DAGs

--dags-path
# Path to the folder where Python DAGs will be generated.

--validate-operators
# Ensure Airflow operators are installed by trying to import them before writing to Python.
# Flag: no value required

--generators-folder
# Path to your Python module with custom Generators

--generators-params
# Object with default values for the desired Generator(s)
# For example: {"AirbyteGenerator": {"host": "http://localhost", "port": "8000", "api_key": "<your-api-key>"}}

--secrets-path
# Secret files location for DAG configuration, i.e. 'yml_path/secrets/'
# Secret content must match the YML dag spec of `nodes -> node_name -> config`

--secrets-manager
# Secret credentials provider, i.e. 'datacoves'
# Mutually exclusive with --secrets-path

--secrets-url
# Secret credentials provider url

--secrets-token
# Secret credentials provider token

--secrets-environment
# Secret credentials project/environment slug

--secrets-tags
# Comma-separated tags to filter which secrets are fetched from the secrets manager

--secrets-key
# Fetch a single secret by key from the secrets manager
```
