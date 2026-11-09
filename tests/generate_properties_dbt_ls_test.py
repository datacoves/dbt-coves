"""
`generate properties` lists models with a separate `dbt ls` process. It must get the
same project/profile/target flags dbt-coves itself was given (#658).
"""

import json
import os
import subprocess
from types import SimpleNamespace

import pytest

from dbt_coves.tasks.generate.properties import GeneratePropertiesTask


def make_task(tmp_path, select="my_model", **args):
    task = GeneratePropertiesTask.__new__(GeneratePropertiesTask)
    task.json_from_dbt_ls = None
    task.args = SimpleNamespace(
        PROFILES_DIR=args.get("PROFILES_DIR"),
        profile=args.get("profile"),
        target=args.get("target"),
        vars=args.get("vars", "{}"),
        TARGET_PATH=args.get("TARGET_PATH"),
    )
    task.config = SimpleNamespace(project_root=str(tmp_path), target_path="target")
    task.coves_config = SimpleNamespace(
        integrated={"generate": {"properties": {"select": select}}}
    )
    return task


@pytest.fixture
def dbt_ls(monkeypatch):
    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        line = json.dumps({"resource_type": "model", "unique_id": "model.p.my_model"})
        return subprocess.CompletedProcess(cmd, 0, stdout=line + "\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    monkeypatch.delenv("DBT_TARGET_PATH", raising=False)
    return calls


def test_dbt_ls_gets_target_and_connection_flags(tmp_path, dbt_ls):
    task = make_task(
        tmp_path,
        PROFILES_DIR="/profiles",
        profile="my_profile",
        target="dev_other",
        vars="{key: value}",
    )
    models = task.list_from_dbt_ls("json")

    assert models == [{"resource_type": "model", "unique_id": "model.p.my_model"}]
    assert dbt_ls[0] == [
        "dbt",
        "ls",
        "--output",
        "json",
        "--resource-type",
        "model",
        "--profiles-dir",
        "/profiles",
        "--profile",
        "my_profile",
        "--target",
        "dev_other",
        "--vars",
        "{key: value}",
        "--select",
        "my_model",
    ]


@pytest.mark.parametrize("empty_vars", ["{}", {}, None])
def test_dbt_ls_omits_unset_flags(tmp_path, dbt_ls, empty_vars):
    make_task(tmp_path, vars=empty_vars).list_from_dbt_ls("json")
    assert dbt_ls[0] == [
        "dbt",
        "ls",
        "--output",
        "json",
        "--resource-type",
        "model",
        "--select",
        "my_model",
    ]


def test_dict_vars_are_passed_as_json(tmp_path, dbt_ls):
    make_task(tmp_path, vars={"key": "value"}).list_from_dbt_ls("json")
    cmd = dbt_ls[0]
    assert cmd[cmd.index("--vars") + 1] == '{"key": "value"}'


def write_manifest(root, target_path):
    path = os.path.join(root, target_path)
    os.makedirs(path, exist_ok=True)
    with open(os.path.join(path, "manifest.json"), "w") as f:
        json.dump({"target_path": target_path}, f)


def test_manifest_read_from_default_target_path(tmp_path, dbt_ls):
    write_manifest(tmp_path, "target")
    assert make_task(tmp_path).load_manifest_nodes() == {"target_path": "target"}


def test_target_path_flag_used_for_dbt_ls_and_manifest(tmp_path, dbt_ls):
    write_manifest(tmp_path, "custom_target")
    task = make_task(tmp_path, TARGET_PATH="custom_target")
    task.list_from_dbt_ls("json")
    cmd = dbt_ls[0]
    assert cmd[cmd.index("--target-path") + 1] == "custom_target"
    assert task.load_manifest_nodes() == {"target_path": "custom_target"}


def test_dbt_target_path_env_var_used(tmp_path, dbt_ls, monkeypatch):
    write_manifest(tmp_path, "env_target")
    monkeypatch.setenv("DBT_TARGET_PATH", "env_target")
    assert make_task(tmp_path).load_manifest_nodes() == {"target_path": "env_target"}
