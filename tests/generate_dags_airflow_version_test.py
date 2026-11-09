"""
`generate airflow-dags` needs the Airflow major version (#672): DAGs generated for
Airflow 2 and 3 aren't interchangeable.
"""

import pytest

from dbt_coves.core.exceptions import DbtCovesException
from dbt_coves.tasks.generate.airflow_dags import (
    GenerateAirflowDagsException,
    GenerateAirflowDagsTask,
)


def make_task(airflow_version):
    task = GenerateAirflowDagsTask.__new__(GenerateAirflowDagsTask)
    task.airflow_version = airflow_version
    task.warned_airflow_3_moved = set()
    task.secrets_path = None
    task.secrets_manager = None
    task.secret_data = None
    task.validate_operators = False
    return task


@pytest.mark.parametrize(
    "configured", [None, 1, 4, "4", "v3", "3.0", True, 3.0, "²", "", [3]]
)
def test_airflow_version_is_required(configured):
    task = make_task(None)
    task.get_config_value = lambda key: configured
    with pytest.raises(DbtCovesException, match="--airflow-version"):
        task._get_airflow_version()


@pytest.mark.parametrize("configured, version", [(2, 2), (3, 3), ("3", 3), (" 2 ", 2)])
def test_airflow_version_accepted(configured, version):
    # From config.yml it can be a string, e.g. through `{{ env_var(...) }}`
    task = make_task(None)
    task.get_config_value = lambda key: configured
    assert task._get_airflow_version() == version


def test_config_model_leaves_validation_to_the_task():
    # A bad value reaches the task's own error message instead of failing the
    # config load with a pydantic ValidationError
    from dbt_coves.config.config import GenerateAirflowDagsModel

    assert GenerateAirflowDagsModel(airflow_version="v3").airflow_version == "v3"


DAG_YML = {
    "schedule": "@daily",
    "nodes": {
        "group": {
            "type": "task_group",
            "tasks": {
                "hello": {
                    "type": "task",
                    "task_decorator": "bash",
                    "bash_command": "echo hi",
                }
            },
        }
    },
}


def generate(task, tmp_path, yml_dag):
    destination = tmp_path / "my_dag.py"
    task.build_dag_file(destination, "my_dag", yml_dag)
    return destination.read_text()


@pytest.mark.parametrize(
    "version, module", [(2, "airflow.decorators"), (3, "airflow.sdk")]
)
def test_decorators_imported_for_the_version(version, module, tmp_path):
    import copy

    written = generate(make_task(version), tmp_path, copy.deepcopy(DAG_YML))
    # isort merges the three imports
    assert f"from {module} import dag, task, task_group\n" in written
    other = {"airflow.decorators", "airflow.sdk"} - {module}
    assert not any(f"from {m} import" in written for m in other)


def test_airflow_3_renames_dag_args_keeping_order(tmp_path, capsys):
    yml_dag = {
        "description": "d",
        "schedule_interval": "@daily",
        "fail_stop": True,
        "concurrency": 2,
        "tags": ["x"],
        "nodes": {},
    }
    task = make_task(3)
    task.generated_groups, task.collected_dependencies = {}, []
    adapted = task._adapt_dag_args(
        "my_dag", {k: v for k, v in yml_dag.items() if k != "nodes"}
    )
    assert list(adapted) == [
        "description",
        "schedule",
        "fail_fast",
        "max_active_tasks",
        "tags",
    ]
    assert adapted["schedule"] == "@daily"
    out = capsys.readouterr().out
    assert "schedule_interval is schedule in Airflow 3" in out


def test_airflow_3_timetable_becomes_schedule():
    adapted = make_task(3)._adapt_dag_args("d", {"timetable": "x"})
    assert adapted == {"schedule": "x"}


def test_airflow_2_renames_fail_fast():
    adapted = make_task(2)._adapt_dag_args("d", {"fail_fast": True, "schedule": None})
    assert adapted == {"fail_stop": True, "schedule": None}


@pytest.mark.parametrize(
    "version, yml_dag",
    [
        (3, {"schedule_interval": "@daily", "schedule": "@hourly"}),
        (2, {"fail_fast": True, "fail_stop": True}),
    ],
)
def test_both_old_and_new_name_is_an_error(version, yml_dag):
    with pytest.raises(GenerateAirflowDagsException, match="keep one"):
        make_task(version)._adapt_dag_args("d", yml_dag)


@pytest.mark.parametrize(
    "version, arg",
    [
        (3, "default_view"),
        (3, "orientation"),
        (2, "deadline"),
        (2, "allowed_run_types"),
    ],
)
def test_args_without_equivalent_are_an_error(version, arg):
    with pytest.raises(GenerateAirflowDagsException, match=arg):
        make_task(version)._adapt_dag_args("d", {arg: "x"})


def test_args_both_versions_know_are_untouched():
    yml_dag = {"schedule": "@daily", "catchup": False, "max_active_tasks": 3}
    for version in (2, 3):
        assert make_task(version)._adapt_dag_args("d", dict(yml_dag)) == yml_dag


@pytest.mark.parametrize("version, warned", [(3, True), (2, False)])
def test_core_operator_paths_warn_on_airflow_3(version, warned, capsys):
    task = make_task(version)
    task.dag_output = {"imports": []}
    task._add_operator_import_to_output("airflow.operators.bash.BashOperator")
    assert task.dag_output["imports"] == [
        "from airflow.operators.bash import BashOperator\n"
    ]
    assert ("apache-airflow-providers-standard" in capsys.readouterr().out) is warned


def test_core_operator_warning_printed_once(capsys):
    task = make_task(3)
    task.dag_output = {"imports": []}
    for _ in range(3):
        task._add_operator_import_to_output("airflow.operators.bash.BashOperator")
    assert capsys.readouterr().out.count("is deprecated in Airflow 3") == 1


def test_two_old_names_for_one_new_name_names_both():
    with pytest.raises(
        GenerateAirflowDagsException, match="sets both schedule_interval and timetable"
    ):
        make_task(3)._adapt_dag_args("d", {"schedule_interval": "x", "timetable": "y"})


@pytest.mark.parametrize(
    "reference, warned",
    [
        ("airflow.operators.BashOperator", True),
        ("airflow.sensors.time_sensor.TimeSensor", True),
        ("from airflow.operators.python import PythonOperator", True),
        ("airflow.providers.standard.operators.bash.BashOperator", False),
        ("from airflow.providers.standard.sensors.time import TimeSensor", False),
        ("my_company.airflow.operators_lib.Custom", False),
        # A user's own package with airflow.operators inside its path
        ("orchestrate.airflow.operators.custom.MyOp", False),
        ("from orchestrate.airflow.sensors import MySensor", False),
        ("import airflow.sensors.base", True),
        # Airflow 3 also deprecates the decorators module
        ("from airflow.decorators import task", True),
        ("from airflow.sdk import Variable", False),
    ],
)
def test_airflow_3_deprecated_modules(reference, warned, capsys):
    make_task(3)._check_airflow_version_compat(reference)
    assert ("is deprecated in Airflow 3" in capsys.readouterr().out) is warned


@pytest.mark.parametrize(
    "reference",
    [
        "from airflow.sdk import Variable",
        "import airflow.sdk",
        "airflow.sdk.BaseOperator",
    ],
)
def test_airflow_sdk_is_an_error_for_airflow_2(reference):
    with pytest.raises(GenerateAirflowDagsException, match="needs Airflow 3"):
        make_task(2)._check_airflow_version_compat(reference)


@pytest.mark.parametrize(
    "reference",
    ["from airflow.decorators import task", "airflow.operators.bash.BashOperator"],
)
def test_airflow_2_paths_are_fine_for_airflow_2(reference, capsys):
    make_task(2)._check_airflow_version_compat(reference)
    assert capsys.readouterr().out == ""


def test_airflow_sdk_import_skips_an_airflow_2_dag(tmp_path):
    yml_dag = {
        "imports": ["from airflow.sdk import Variable"],
        "nodes": {"t": {"type": "task", "task_decorator": "bash"}},
    }
    with pytest.raises(GenerateAirflowDagsException, match="needs Airflow 3"):
        generate(make_task(2), tmp_path, yml_dag)
    assert not (tmp_path / "my_dag.py").exists()


def test_notifications_as_a_list_reaches_the_format_error(tmp_path):
    # Not a mapping: the behavior check leaves it to generate_notifiers' error
    make_task(3)._adapt_dag_args("d", {"schedule": None, "notifications": [{"a": 1}]})


def test_custom_imports_are_checked_too(tmp_path, capsys):
    yml_dag = {
        "imports": ["from airflow.operators.python import PythonOperator"],
        "nodes": {"t": {"type": "task", "task_decorator": "bash"}},
    }
    generate(make_task(3), tmp_path, yml_dag)
    out = " ".join(capsys.readouterr().out.split())
    assert "from airflow.operators.python import PythonOperator is deprecated" in out


@pytest.mark.parametrize(
    "operator, valid",
    [
        ("datetime.datetime", True),
        ("datetime.NoSuchClass", False),
        ("no_such_module_xyz.Operator", False),
    ],
)
def test_validate_operators_checks_the_class(operator, valid):
    task = make_task(2)
    task.validate_operators = True
    task.dag_output = {"imports": []}
    if valid:
        task._add_operator_import_to_output(operator)
        assert task.dag_output["imports"]
    else:
        with pytest.raises(GenerateAirflowDagsException, match="Can't import operator"):
            task._add_operator_import_to_output(operator)


def test_fail_stop_rename_mentions_airflow_2_7(capsys):
    make_task(2)._adapt_dag_args("d", {"fail_fast": True, "schedule": None})
    out = " ".join(capsys.readouterr().out.split())
    assert "fail_fast is fail_stop in Airflow 2.7+, renamed" in out


@pytest.mark.parametrize(
    "version, dag_args, warned",
    [
        (3, {"catchup": False}, True),
        (3, {"schedule": None}, False),
        (3, {"schedule_interval": "@daily"}, False),  # renamed to schedule
        (2, {"catchup": False}, False),
    ],
)
def test_airflow_3_warns_when_no_schedule(version, dag_args, warned, capsys):
    make_task(version)._adapt_dag_args("d", dag_args)
    out = " ".join(capsys.readouterr().out.split())
    assert ("no schedule set" in out) is warned


@pytest.mark.parametrize(
    "version, dag_args, warned",
    [
        (3, {"schedule": None, "sla_miss_callback": "cb"}, True),
        (
            3,
            {
                "schedule": None,
                "notifications": {"sla_miss_callback": {"notifier": "a.B"}},
            },
            True,
        ),
        (3, {"schedule": None, "notifications": {"on_failure_callback": {}}}, False),
        (2, {"schedule": None, "sla_miss_callback": "cb"}, False),
    ],
)
def test_airflow_3_warns_sla_miss_callback_has_no_effect(
    version, dag_args, warned, capsys
):
    make_task(version)._adapt_dag_args("d", dag_args)
    out = " ".join(capsys.readouterr().out.split())
    assert ("sla_miss_callback has no effect in Airflow 3" in out) is warned
