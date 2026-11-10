"""
blue-green's own Snowflake connection: closed when the run ends, and set up so
snowflake-connector-python 5.x doesn't warn on every run.
"""

from types import SimpleNamespace

import pytest

from dbt_coves.tasks.blue_green import main
from dbt_coves.tasks.blue_green.main import BlueGreenTask


class FakeConnection:
    closed = False

    def close(self):
        self.closed = True


def make_task(monkeypatch):
    task = BlueGreenTask.__new__(BlueGreenTask)
    task.get_config_value = lambda key: (
        "PROD_DB_VAR" if key == "prod_db_env_var" else None
    )
    monkeypatch.setenv("PROD_DB_VAR", "PROD")
    task.args = SimpleNamespace(uuid=None)  # usage tracking off
    task.con_used = FakeConnection()
    task.snowflake_connection = lambda: task.con_used
    return task


def test_connection_closed_after_a_successful_run(monkeypatch):
    task = make_task(monkeypatch)
    task._deploy = lambda: 0
    assert task.run() == 0
    assert task.con_used.closed


def test_connection_closed_when_the_run_fails(monkeypatch):
    task = make_task(monkeypatch)

    def failing_deploy():
        raise RuntimeError("dbt build failed")

    task._deploy = failing_deploy
    with pytest.raises(RuntimeError, match="dbt build failed"):
        task.run()
    assert task.con_used.closed


def test_a_failing_close_does_not_hide_the_runs_error(monkeypatch, caplog):
    task = make_task(monkeypatch)

    def failing_close():
        raise ConnectionError("logout failed")

    task.con_used.close = failing_close

    def failing_deploy():
        raise RuntimeError("dbt build failed")

    task._deploy = failing_deploy
    with pytest.raises(RuntimeError, match="dbt build failed"):
        task.run()
    # ...but the open session is reported
    assert any(
        r.levelname == "WARNING"
        and "Couldn't close the Snowflake connection" in r.message
        for r in caplog.records
    )


@pytest.mark.parametrize("major, expected", [(4, None), (5, False), (6, False)])
def test_keep_alive_auto_detection_only_set_for_connector_5(
    monkeypatch, major, expected
):
    # False: always log out on close, even if a failed query left others running
    monkeypatch.setattr(main, "_snowflake_connector_major_version", lambda: major)
    task = BlueGreenTask.__new__(BlueGreenTask)
    task.config = SimpleNamespace(
        credentials=SimpleNamespace(
            account="a",
            warehouse="w",
            database="d",
            role="r",
            schema="s",
            user="u",
            password="p",
            private_key_path=None,
            private_key=None,
        )
    )
    connection = task._get_snowflake_credentials_from_dbt_adapter()
    assert (
        connection.get("enable_server_session_keep_alive_auto_detection", None)
        is expected
    )
    assert ("enable_server_session_keep_alive_auto_detection" in connection) is (
        major >= 5
    )


def test_connector_major_version_reads_the_imported_module():
    import snowflake.connector

    expected = int(snowflake.connector.__version__.split(".")[0])
    assert main._snowflake_connector_major_version() == expected
