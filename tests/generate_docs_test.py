import os
import subprocess
from pathlib import Path

import pytest


@pytest.fixture(scope="session")
def dbt_project_path():
    return Path(__file__).parent.resolve() / "generate_docs_cases"


def test_generate_docs(dbt_project_path):
    """
    Test that runs `dbt-coves generate docs`, fills the target folder and asserts no errors result.
    """
    command = [
        "python",
        "../../dbt_coves/core/main.py",
        "generate",
        "docs",
    ]
    subprocess.run(
        command,
        check=True,
        cwd=dbt_project_path,
    )
    assert os.path.exists(dbt_project_path / "target" / "index.html")


def test_generate_docs_merge_deferred(dbt_project_path):
    """
    Test that runs `dbt-coves generate docs --merge-deferred --state state/path`
    Asserts state/catalog and target/catalog were merged
    """
    command = [
        "python",
        "../../dbt_coves/core/main.py",
        "generate",
        "docs",
        "--merge-deferred",
        "--state",
        "state",
    ]
    output = subprocess.check_output(command, cwd=dbt_project_path)
    decoded = output.decode("utf-8")
    assert "SUCCESS" in decoded
    assert "into your local catalog.json" in decoded


def test_generate_docs_merge_deferred_no_state(dbt_project_path):
    """
    Test that runs `dbt-coves generate docs --merge-deferred`
    It's expected to break because no state was passed
    """
    command = ["python", "../../dbt_coves/core/main.py", "generate", "docs", "--merge-deferred"]
    try:
        output = subprocess.check_output(command, cwd=dbt_project_path)
    except subprocess.CalledProcessError as e:
        assert (
            "A valid --state argument is required when using --merge-deferred"
            in e.output.decode("utf-8")
        )


def test_generate_docs_dbt_args(dbt_project_path):
    """
    Run `dbt-coves generate docs --dbt-args "--no-compile"`
    """
    command = [
        "python",
        "../../dbt_coves/core/main.py",
        "generate",
        "docs",
        "--dbt-args",
        "--no-compile",
    ]
    subprocess.run(
        command,
        check=True,
        cwd=dbt_project_path,
    )
    assert os.path.exists(dbt_project_path / "target" / "index.html")
