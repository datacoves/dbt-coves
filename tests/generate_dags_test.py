import os
import subprocess
import unittest
from pathlib import Path

import pytest
from airflow.models import DagBag


@pytest.fixture
def test_data_dir():
    return Path(__file__).parent / "generate_dags_cases" / "input"


@pytest.fixture
def test_output_dir():
    return Path(__file__).parent / "generate_dags_cases" / "output"


def test_generate_airflow_dags(test_data_dir: Path, test_output_dir: Path):
    """
    Test that runs `dbt-coves generate airflow-dags`, fills Airflow's DagBag and asserts no errors result.
    """
    command = [
        "python",
        "../dbt_coves/core/main.py",
        "generate",
        "airflow-dags",
        "--yml-path",
        test_data_dir,
        "--dag-path",
        test_output_dir,
    ]
    subprocess.run(
        command,
        check=True,
        cwd=Path(__file__).parent.resolve(),
    )
    dag_bag = DagBag(test_output_dir, include_examples=False)
    for file in os.listdir(test_output_dir):
        os.remove(test_output_dir / file)
    assert not dag_bag.import_errors
