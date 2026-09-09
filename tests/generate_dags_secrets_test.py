import pytest
import requests

from dbt_coves.core.exceptions import DbtCovesException
from dbt_coves.tasks.generate.airflow_dags import (
    GenerateAirflowDagsException,
    GenerateAirflowDagsTask,
)
from dbt_coves.utils.secrets import (
    DATACOVES_SECRETS_SETTINGS,
    contains_secret,
    load_secret_manager_data,
    replace_secrets,
)


class FakeTask:
    """A task instance as `load_secret_manager_data` consumes one"""

    secrets_manager = "datacoves"

    def __init__(self, config=None):
        self.config = config or {}

    def get_config_value(self, key):
        return self.config.get(key)


@pytest.fixture
def task():
    """A DAG generation task with a secrets manager configured, and no secrets loaded"""
    task = GenerateAirflowDagsTask.__new__(GenerateAirflowDagsTask)
    task.secrets_path = None
    task.secrets_manager = "datacoves"
    task.secret_data = None
    task.secret_data_error = None
    task.skipped_dags = []
    return task


@pytest.fixture
def loads(monkeypatch):
    """Record every secrets manager fetch the task performs"""
    calls = []

    def fake_load(task_instance):
        calls.append(task_instance)
        return [{"slug": "my_secret", "value": "s3cret"}]

    monkeypatch.setattr(
        "dbt_coves.tasks.generate.airflow_dags.load_secret_manager_data", fake_load
    )
    return calls


@pytest.mark.parametrize(
    "value, expected",
    [
        ("{{ secret('my_secret') }}", True),
        ("{{SECRET('my_secret')}}", True),
        ("plain value", False),
        ({"nodes": {"task": {"password": "{{ secret('my_secret') }}"}}}, True),
        ({"nodes": {"task": {"type": "task"}}}, False),
        ({"args": ["a", ["{{ secret('my_secret') }}"]]}, True),
        ({"retries": 3, "start_date": None}, False),
    ],
)
def test_contains_secret(value, expected):
    assert contains_secret(value) is expected


def test_dag_without_secrets_skips_the_manager(task, loads):
    yml_dag = {"nodes": {"my_task": {"type": "task", "operator": "some.Operator"}}}

    assert task._discover_secrets(yml_dag) == yml_dag
    assert not loads


def test_dag_with_secrets_uses_the_manager(task, loads):
    yml_dag = {"nodes": {"my_task": {"password": "{{ secret('my_secret') }}"}}}

    assert task._discover_secrets(yml_dag) == {
        "nodes": {"my_task": {"password": "s3cret"}}
    }
    assert len(loads) == 1


def test_manager_is_contacted_once_per_run(task, loads):
    for _ in range(3):
        task._discover_secrets({"nodes": {"t": {"pwd": "{{ secret('my_secret') }}"}}})

    assert len(loads) == 1


def test_unresolved_secrets_are_an_error(task, monkeypatch):
    """A DAG asking for a secret the manager doesn't hold must not ship the placeholder"""
    monkeypatch.setattr(
        "dbt_coves.tasks.generate.airflow_dags.load_secret_manager_data",
        lambda task_instance: [],
    )

    with pytest.raises(DbtCovesException, match="my_secret"):
        task._discover_secrets({"nodes": {"t": {"pwd": "{{ secret('my_secret') }}"}}})


def test_secrets_nested_in_lists_are_replaced():
    config = {
        "args": ["--password", "{{ secret('my_secret') }}", ["{{ secret('a') }}"]]
    }

    replace_secrets(
        [{"slug": "my_secret", "value": "s3cret"}, {"slug": "a", "value": "nested"}],
        config,
    )

    assert config == {"args": ["--password", "s3cret", ["nested"]]}


def test_a_top_level_list_is_replaced_in_place():
    config = ["{{ secret('my_secret') }}"]

    replace_secrets([{"slug": "my_secret", "value": "s3cret"}], config)

    assert config == ["s3cret"]


def test_a_dag_that_cannot_resolve_its_secrets_is_skipped(task, tmp_path, monkeypatch):
    """One DAG's unresolvable secret must not stop the DAGs still to be generated"""
    monkeypatch.setattr(
        "dbt_coves.tasks.generate.airflow_dags.load_secret_manager_data",
        lambda task_instance: [],
    )
    yml_filepath = tmp_path / "my_dag.yml"
    yml_filepath.write_text(
        "nodes:\n  my_task:\n    pwd: \"{{ secret('missing') }}\"\n"
    )
    task.dags_path = None
    task.ymls_path = tmp_path
    task.yml_dags_path_env = None

    task._generate_dag(yml_filepath)

    assert not (tmp_path / "my_dag.py").exists()
    assert task.skipped_dags == ["my_dag"]  # so the run can exit non-zero


def test_the_manager_is_only_asked_once_when_it_is_unavailable(task, monkeypatch):
    """
    Its settings belong to the run, not to one DAG: report them once rather than
    once per DAG that needs a secret
    """
    attempts = []

    def fake_load(task_instance):
        attempts.append(task_instance)
        raise DbtCovesException("secrets_token must be provided")

    monkeypatch.setattr(
        "dbt_coves.tasks.generate.airflow_dags.load_secret_manager_data", fake_load
    )
    yml_dag = {"nodes": {"t": {"pwd": "{{ secret('my_secret') }}"}}}

    with pytest.raises(GenerateAirflowDagsException, match="secrets_token"):
        task._discover_secrets(yml_dag)
    with pytest.raises(GenerateAirflowDagsException, match="reported above"):
        task._discover_secrets(yml_dag)

    assert len(attempts) == 1


def test_a_manager_that_cannot_be_reached_skips_the_dag(task, monkeypatch):
    """A revoked token or an unreachable host is not this DAG's to fix either"""

    def fake_load(task_instance):
        raise requests.HTTPError("401 Client Error for url: https://secrets.local")

    monkeypatch.setattr(
        "dbt_coves.tasks.generate.airflow_dags.load_secret_manager_data", fake_load
    )

    with pytest.raises(GenerateAirflowDagsException, match="401"):
        task._discover_secrets({"nodes": {"t": {"pwd": "{{ secret('a') }}"}}})


def test_errors_do_not_leak_between_calls():
    with pytest.raises(DbtCovesException):
        replace_secrets([], {"pwd": "{{ secret('missing') }}"})

    replace_secrets([{"slug": "my_secret", "value": "s3cret"}], {"pwd": "plain"})


def test_missing_settings_are_named_in_the_error():
    with pytest.raises(DbtCovesException) as excinfo:
        load_secret_manager_data(FakeTask({"secrets_url": "https://secrets.local"}))

    message = excinfo.value.args[0]
    assert "secrets_url" not in message
    assert "secrets_token" in message and "DATACOVES__SECRETS_TOKEN" in message
    assert "secrets_environment" in message and "DATACOVES__ENVIRONMENT_SLUG" in message


def test_settings_can_come_from_the_environment(monkeypatch):
    for key, env_var in DATACOVES_SECRETS_SETTINGS:
        monkeypatch.setenv(env_var, key)
    calls = {}

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return [{"slug": "my_secret", "value": "s3cret"}]

    def fake_get(url, headers=None, params=None):
        calls.update(url=url, headers=headers)
        return FakeResponse()

    monkeypatch.setattr("dbt_coves.utils.secrets.requests.get", fake_get)

    assert load_secret_manager_data(FakeTask()) == [
        {"slug": "my_secret", "value": "s3cret"}
    ]
    assert calls["url"] == "secrets_url/api/v1/secrets/secrets_environment"
    assert calls["headers"] == {"Authorization": "token secrets_token"}
