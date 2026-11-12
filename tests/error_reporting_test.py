"""
How `dbt-coves` reports errors: its own (and dbt's) errors print a message without
a traceback unless -vv / --log-level debug asks for one; unexpected errors print the
traceback; nothing is printed twice.
"""

import logging
import subprocess
from types import SimpleNamespace

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt_coves.core import main as cli
from dbt_coves.core.exceptions import DbtCovesException
from dbt_coves.tasks.generate.airflow_generators.base import GeneratorException


def run_main(monkeypatch, capsys, error, show_tracebacks=False):
    def handle(parser, cli_args):
        cli._show_tracebacks = show_tracebacks  # as parsed from the flags
        raise error

    monkeypatch.setattr(cli, "handle", handle)
    exit_code = cli.main(test_cli_args=["generate", "airflow-dags"])
    out = " ".join(capsys.readouterr().out.split())  # unwrap rich's line breaks
    return exit_code, out


@pytest.mark.parametrize(
    "error",
    [
        DbtCovesException("[red]needs the Airflow version[/red]"),
        GeneratorException("[red]needs the Airflow version[/red]"),
    ],
)
def test_own_errors_print_the_message_only(monkeypatch, capsys, error):
    exit_code, out = run_main(monkeypatch, capsys, error)
    assert exit_code == 1
    assert "needs the Airflow version" in out
    assert "[red]" not in out  # markup rendered
    assert "Traceback" not in out


def test_own_errors_show_the_traceback_on_request(monkeypatch, capsys):
    error = DbtCovesException("needs the Airflow version")
    _, out = run_main(monkeypatch, capsys, error, show_tracebacks=True)
    assert "Traceback" in out
    assert out.count("needs the Airflow version") == 2  # traceback, then message


def test_invalid_markup_in_a_message_is_printed_as_is(monkeypatch, capsys):
    exit_code, out = run_main(
        monkeypatch, capsys, DbtCovesException("closing tag [/b] without opening")
    )
    assert exit_code == 1
    assert "closing tag [/b] without opening" in out


def test_dbt_errors_print_the_message_only(monkeypatch, capsys):
    error = DbtRuntimeError("Database Error: Incorrect username or password")
    exit_code, out = run_main(monkeypatch, capsys, error)
    assert exit_code == 1
    assert "Incorrect username or password" in out
    assert "Traceback" not in out


def test_failed_command_prints_command_and_output(monkeypatch, capsys):
    error = subprocess.CalledProcessError(
        2, ["dbt", "ls", "--select", "tag:x"], stderr=b"Error: [Errno 2] no profile"
    )
    exit_code, out = run_main(monkeypatch, capsys, error)
    assert exit_code == 2
    assert "dbt ls --select tag:x failed with exit code 2" in out
    assert "Error: [Errno 2] no profile" in out  # not read as markup
    assert "Traceback" not in out


def test_unexpected_errors_print_the_traceback(monkeypatch, capsys):
    exit_code, out = run_main(monkeypatch, capsys, KeyError("[b]oops"))
    assert exit_code == 1
    assert "Traceback" in out
    assert "KeyError: '[b]oops'" in out  # escaped, not read as markup
    assert "github.com/datacoves/dbt-coves/issues" in out


def test_errors_are_logged_once(monkeypatch, capsys):
    # dbt and others configure the root logger: dbt-coves' messages mustn't also
    # reach it, which printed them a second time with their markup unrendered
    root_records = []

    class Collect(logging.Handler):
        def emit(self, record):
            root_records.append(record)

    handler = Collect()
    logging.getLogger().addHandler(handler)
    try:
        monkeypatch.setattr(cli, "handle", lambda parser, cli_args: 1)
        assert cli.main(test_cli_args=["generate", "airflow-dags"]) == 1
    finally:
        logging.getLogger().removeHandler(handler)
    assert root_records == []


@pytest.mark.parametrize(
    "content, expected",
    [
        ("generate: [unclosed\n", "isn't valid YAML"),
        ("generate:\n  sources:\n    update_strategy: [1, 2]\n", "invalid settings"),
    ],
)
def test_invalid_config_is_a_dbt_coves_error(tmp_path, content, expected):
    from dbt_coves.config.config import DbtCovesConfig

    config_path = tmp_path / "config.yml"
    config_path.write_text(content)
    config = DbtCovesConfig.__new__(DbtCovesConfig)
    config._config_path = config_path
    with pytest.raises(DbtCovesException, match=expected):
        config.load_and_validate_config_yaml()


def test_config_that_is_not_a_mapping_is_a_dbt_coves_error(tmp_path):
    from dbt_coves.config.config import DbtCovesConfig

    config_path = tmp_path / "config.yml"
    config_path.write_text("- generate\n")
    config = DbtCovesConfig.__new__(DbtCovesConfig)
    config._config_path = config_path
    with pytest.raises(DbtCovesException, match="must hold settings"):
        config.load_and_validate_config_yaml()


def test_skipped_dag_message_with_invalid_markup_is_printed_as_is(capsys):
    from pathlib import Path

    from dbt_coves.tasks.generate.airflow_dags import GenerateAirflowDagsTask

    task = GenerateAirflowDagsTask.__new__(GenerateAirflowDagsTask)
    task.skipped_dags = []
    task._skip_dag(Path("my_dag.yml"), "API said [/oops]")
    assert "API said [/oops]" in capsys.readouterr().out
    assert task.skipped_dags == ["my_dag"]


def test_airbyte_api_errors_escape_the_server_text(monkeypatch):
    from types import SimpleNamespace

    from dbt_coves.utils import api_caller

    response = SimpleNamespace(
        status_code=500, text="boom", json=lambda: {"message": "[/b] [error]"}
    )
    monkeypatch.setattr(api_caller.requests, "request", lambda *a, **kw: response)
    caller = api_caller.AirbyteApiCaller.__new__(api_caller.AirbyteApiCaller)
    caller.base_url, caller.headers = "http://x", {}
    with pytest.raises(api_caller.AirbyteApiCallerException) as raised:
        caller._request("GET", "workspaces")
    from rich.text import Text

    assert "[/b] [error]" in Text.from_markup(str(raised.value)).plain


def test_cleanup_failure_does_not_hide_the_dbt_error(tmp_path, monkeypatch):
    import subprocess

    from dbt_coves.tasks.generate.airflow_generators import base

    def run(cmd, **kwargs):
        if "rev-parse" in cmd:
            return SimpleNamespace(stdout="abc\n", returncode=0)
        if "ls" in cmd:
            raise subprocess.CalledProcessError(2, cmd, stderr=b"dbt failed")
        if "rm" in cmd:
            if kwargs.get("check"):
                raise subprocess.CalledProcessError(1, cmd)
            return SimpleNamespace(returncode=1)
        return SimpleNamespace(stdout=b"", returncode=0)

    monkeypatch.setattr(base.subprocess, "run", run)
    generator = base.BaseDbtGenerator(dbt_project_path=str(tmp_path))
    generator.is_readonly = lambda folder: True
    with pytest.raises(base.GeneratorException, match="dbt failed"):
        generator.discover_dbt_connections()
