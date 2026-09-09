import datetime

import pytest
import yaml

from dbt_coves.tasks.generate.airflow_dags import (
    GenerateAirflowDagsException,
    GenerateAirflowDagsTask,
    RawExpr,
)


@pytest.fixture
def task():
    """A DAG generation task ready to render nodes, with no secrets configured"""
    task = GenerateAirflowDagsTask.__new__(GenerateAirflowDagsTask)
    task.secrets_path = None
    task.secrets_manager = None
    task.secret_data = None
    task.generated_groups = {}
    task.collected_dependencies = []
    task.dag_output = {"docstring": [], "imports": [], "globals": [], "dag": []}
    return task


def load_yml(task, source: str):
    """Load YML through the task's constructors, as a generated DAG's file is"""
    yaml.FullLoader.add_constructor("!py", task.raw_expr_constructor)
    return yaml.full_load(source)


def test_task_without_decorator_args_has_no_stray_comma(task):
    """A `,` on its own line is a parse error: leave the argument line out"""
    output = "".join(
        task.generate_task_output(
            "say_hello", {"task_decorator": "datacoves_bash", "bash_command": "echo hi"}
        )
    )

    assert "@task.datacoves_bash(\n    )\n" in output
    assert ",\n" not in output


def test_task_decorator_args_are_still_rendered(task):
    output = "".join(
        task.generate_task_output(
            "run_dbt",
            {"task_decorator": "datacoves_dbt", "connection_id": "main_key_pair"},
        )
    )

    assert 'connection_id="main_key_pair",\n' in output


def test_raw_expressions_are_unquoted_at_node_level(task):
    """`!py` on a task argument must emit the expression, not a string"""
    output = "".join(
        task.generate_task_output(
            "run_dbt",
            {
                "task_decorator": "datacoves_dbt",
                "execution_timeout": RawExpr("pendulum.duration(minutes=15)"),
            },
        )
    )

    assert "execution_timeout=pendulum.duration(minutes=15),\n" in output


def test_a_tagged_quoted_scalar_holding_a_dict_literal(task):
    """
    The only YAML shape that can carry an expression containing `": "`, so it's
    the one real usage needs
    """
    node_conf = load_yml(
        task,
        "task_decorator: datacoves_bash\n"
        'env: !py \'datacoves_utils.set_dlt_env_vars({"destinations": ["main"]})\'\n',
    )

    output = "".join(task.generate_task_output("load_data", node_conf))

    assert (
        'env=datacoves_utils.set_dlt_env_vars({"destinations": ["main"]}),\n' in output
    )


def test_a_py_tag_inside_the_quotes_is_warned_about(task, capsys):
    node_conf = load_yml(
        task, "task_decorator: datacoves_bash\nenv: '!py datacoves_utils.set_env()'\n"
    )

    task.generate_task_output("load_data", node_conf)

    assert "!py" in capsys.readouterr().out


def test_a_scalar_datetime_renders_as_python(task):
    """`str()` on a datetime gives `2023-01-01 00:00:00`, which doesn't parse"""
    dag_args = task.dag_args_to_string({"start_date": datetime.datetime(2023, 1, 1)})

    assert dag_args.strip() == "start_date=datetime.datetime(2023, 1, 1, 0, 0),"
    assert "import datetime\n" in task.dag_output["imports"]


def test_a_datetime_nested_in_a_node_argument_imports_datetime(task):
    """It renders through the dict's repr, but the import still has to be there"""
    task.generate_task_output(
        "say_hello",
        {
            "task_decorator": "datacoves_bash",
            "default_args": {"start_date": datetime.datetime(2023, 1, 1)},
        },
    )

    assert "import datetime\n" in task.dag_output["imports"]


def test_a_raw_expression_nested_in_a_mapping_stays_unquoted(task):
    output = "".join(
        task.generate_task_output(
            "say_hello",
            {
                "task_decorator": "datacoves_bash",
                "opts": {"on_failure": RawExpr("my_callback")},
            },
        )
    )

    assert "opts={'on_failure': my_callback}," in output


def test_raw_expressions_are_unquoted_in_notifier_args(task):
    output = task.generate_notifiers(
        {
            "on_failure_callback": {
                "notifier": "dbt_coves.notifications.slack.SlackNotifier",
                "args": {"text": "failed", "channel": RawExpr("my_channel")},
            }
        }
    )

    assert 'text="failed"' in output[0]
    assert "channel=my_channel" in output[0]


def test_a_k8s_config_is_a_global_not_a_string(task):
    """`executor_config` names the global just emitted, so it can't be quoted"""
    output = "".join(
        task.generate_task_output(
            "say_hello",
            {
                "task_decorator": "datacoves_bash",
                "config": {"resources": {"requests": {"cpu": "1"}}},
            },
        )
    )

    assert "executor_config=SAY_HELLO_CONFIG,\n" in output


def test_unreadable_yml_skips_only_that_dag(task, tmp_path, capsys):
    """A tab-indented file is one DAG's problem, not the run's"""
    yml_filepath = tmp_path / "broken.yml"
    yml_filepath.write_text("nodes:\n\tmy_task:\n\t\ttype: task\n")
    task.dags_path = None
    task.ymls_path = tmp_path
    task.yml_dags_path_env = None
    task.skipped_dags = []

    task._generate_dag(yml_filepath)

    assert task.skipped_dags == ["broken"]
    assert not (tmp_path / "broken.py").exists()


def test_the_error_survives_rich_markup(task, tmp_path):
    """
    Black echoes the line it choked on, and `[...]` in it would be read as markup
    and dropped -- and it is the only diagnostic now that nothing is written
    """
    destination_path = tmp_path / "my_dag.py"

    with pytest.raises(GenerateAirflowDagsException) as excinfo:
        task.build_dag_file(
            destination_path=destination_path,
            dag_name="my_dag",
            yml_dag={
                "nodes": {
                    "t": {
                        "type": "task",
                        "task_decorator": "datacoves_bash",
                        "broken": RawExpr("[a b]("),
                    }
                }
            },
        )

    assert r"\[a b](" in str(excinfo.value)


def test_an_invalid_dag_leaves_the_destination_alone(task, tmp_path):
    """
    The console says the DAG was skipped, so the file that was there must still
    be there -- unparseable output used to replace it
    """
    destination_path = tmp_path / "my_dag.py"
    destination_path.write_text("# the DAG that worked\n")

    with pytest.raises(GenerateAirflowDagsException, match="left unchanged"):
        task.build_dag_file(
            destination_path=destination_path,
            dag_name="my_dag",
            yml_dag={
                "nodes": {
                    "t": {
                        "type": "task",
                        "task_decorator": "datacoves_bash",
                        "broken": RawExpr("("),
                    }
                }
            },
        )

    assert destination_path.read_text() == "# the DAG that worked\n"


def test_a_valid_dag_is_written(task, tmp_path):
    destination_path = tmp_path / "my_dag.py"

    task.build_dag_file(
        destination_path=destination_path,
        dag_name="my_dag",
        yml_dag={
            "schedule": "@daily",
            "nodes": {
                "say_hello": {
                    "type": "task",
                    "task_decorator": "datacoves_bash",
                    "bash_command": "echo hi",
                }
            },
        },
    )

    written = destination_path.read_text()
    assert "@task.datacoves_bash()" in written
    assert 'schedule="@daily"' in written
