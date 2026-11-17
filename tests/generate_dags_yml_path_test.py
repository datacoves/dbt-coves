"""
--yml-path problems used to surface as a Python error from inside DAG generation, e.g.
"ValueError: '/nope' is not in the subpath of '/config/workspace/orchestrate/...'".
"""

from pathlib import Path
from types import SimpleNamespace

import pytest

from dbt_coves.tasks.generate.airflow_dags import (
    GenerateAirflowDagsException,
    GenerateAirflowDagsTask,
)

DATACOVES_YML_FOLDER = Path("/config/workspace/orchestrate/dags_yml_definitions")


def make_task(config):
    task = GenerateAirflowDagsTask.__new__(GenerateAirflowDagsTask)
    task.args = SimpleNamespace(uuid=None, disable_tracking=True)
    task.coves_config = SimpleNamespace(disable_tracking=True)
    task.get_config_value = lambda key: config.get(key)
    return task


def test_missing_yml_path_is_a_clear_error(tmp_path):
    missing = tmp_path / "nope"
    task = make_task(
        {"airflow_version": 3, "yml_path": str(missing), "dags_path": str(tmp_path)}
    )
    with pytest.raises(GenerateAirflowDagsException, match="doesn't exist") as exc:
        task.run()
    assert str(missing) in str(exc.value)
    assert list(tmp_path.iterdir()) == []


def destination(yml_file, ymls_path, dags_path, yml_dags_path_env=None):
    task = make_task({})
    task.dags_path = dags_path
    task.ymls_path = Path(ymls_path)
    task.yml_dags_path_env = yml_dags_path_env
    return task._dag_destination(Path(yml_file))


def test_folder_keeps_subfolders(tmp_path):
    dags = tmp_path / "dags"
    assert destination("/ymls/team/a.yml", "/ymls", str(dags)) == dags / "team/a.py"


def test_single_file_in_datacoves_folder_keeps_subfolders(tmp_path):
    # The Datacoves VS Code task passes the open YML file as --yml-path
    dags = tmp_path / "dags"
    yml = DATACOVES_YML_FOLDER / "team/a.yml"
    assert (
        destination(yml, yml, str(dags), "orchestrate/dags_yml_definitions")
        == dags / "team/a.py"
    )


def test_single_file_outside_datacoves_folder_goes_to_dags_path(tmp_path):
    dags = tmp_path / "dags"
    yml = tmp_path / "elsewhere/a.yml"
    assert (
        destination(yml, yml, str(dags), "orchestrate/dags_yml_definitions")
        == dags / "a.py"
    )


def test_single_file_without_datacoves(tmp_path):
    dags = tmp_path / "dags"
    yml = tmp_path / "elsewhere/a.yml"
    assert destination(yml, yml, str(dags)) == dags / "a.py"


def test_no_dags_path_writes_next_to_the_yml():
    assert destination("/ymls/a.yml", "/ymls/a.yml", None) == Path("/ymls/a.py")
