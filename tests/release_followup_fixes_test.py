"""
Small fixes from the 2026-11 release follow-ups:
- `generate metadata` quotes its JSON-sampling query on Snowflake, like `generate
  sources` (#681).
- `generate sources --flatten-json-fields yes` still generates relations without
  JSON columns.
- `generate airflow-dags` generators read manifest.json from dbt's target path.
"""

from types import SimpleNamespace

import pytest

from dbt_coves.tasks.generate.airflow_generators.base import BaseDbtGenerator
from dbt_coves.tasks.generate.metadata import GenerateMetadataTask
from dbt_coves.tasks.generate.sources import GenerateSourcesTask


def fake_adapter(adapter_type, executed):
    def execute(sql, fetch):
        executed.append(sql)
        return None, SimpleNamespace(rows=[])

    return SimpleNamespace(
        type=lambda: adapter_type, quote=lambda name: f'"{name}"', execute=execute
    )


@pytest.mark.parametrize("task_class", [GenerateMetadataTask, GenerateSourcesTask])
@pytest.mark.parametrize(
    "adapter_type, expected",
    [
        ("snowflake", 'SELECT "payload" FROM "RAW"."json_t" limit 1'),
        # Other adapters are unchanged
        ("redshift", "SELECT payload FROM RAW.json_t limit 1"),
    ],
)
def test_json_sampling_query(task_class, adapter_type, expected):
    executed = []
    task = task_class.__new__(task_class)
    task.adapter = fake_adapter(adapter_type, executed)
    task.get_config_value = lambda key: None
    task.get_nested_keys(["payload"], SimpleNamespace(schema="RAW", name="json_t"))
    assert executed == [expected]


def test_json_sampling_query_with_configured_database():
    executed = []
    task = GenerateMetadataTask.__new__(GenerateMetadataTask)
    task.adapter = fake_adapter("snowflake", executed)
    task.get_config_value = lambda key: "RAW_DB" if key == "database" else None
    task.get_nested_keys(["payload"], SimpleNamespace(schema="RAW", name="t"))
    assert executed == ['SELECT "payload" FROM RAW_DB."RAW"."t" limit 1']


@pytest.mark.parametrize(
    "dtypes, json_cols",
    # No JSON columns: an empty list, which renders without flattening
    [(["VARIANT", "NUMBER"], ["data"]), (["NUMBER", "VARCHAR"], [])],
)
def test_flatten_yes_generates_relations_without_json_columns(dtypes, json_cols):
    task = GenerateSourcesTask.__new__(GenerateSourcesTask)
    task.flatten_json = "yes"
    columns = [
        SimpleNamespace(name=name, dtype=dtype)
        for name, dtype in zip(["data", "id"], dtypes)
    ]
    task.adapter = SimpleNamespace(get_columns_in_relation=lambda relation: columns)
    task.NESTED_FIELD_TYPES = {"SimpleNamespace": "VARIANT"}
    rendered = []
    task.render_templates = lambda relation, cols, dest, opts, json_cols=None: (
        rendered.append(json_cols)
    )
    destination = SimpleNamespace(parent=SimpleNamespace(mkdir=lambda **kw: None))

    task.generate_model("rel", destination, {"flatten_all": "Yes"})

    # Rendered whether or not there's JSON to flatten (it used to be skipped)
    assert rendered == [json_cols]


@pytest.mark.parametrize(
    "list_args, env, project_yml, expected",
    [
        ("--target-path custom", None, None, "custom"),
        ("-s tag:x --target-path=custom2", None, None, "custom2"),
        ("", "from_env", "target-path: from_project\n", "from_env"),
        ("", None, "target-path: from_project\n", "from_project"),
        ("", None, "name: p\n", "target"),
        ("", None, "", "target"),  # empty dbt_project.yml
        ("", None, None, "target"),
        # dbt renders Jinja here, which dbt-coves can't: the default instead
        ("", None, "target-path: \"{{ env_var('X', 'out') }}\"\n", "target"),
        # Unbalanced quotes are split as the dbt command does
        ("-s 'tag:x --target-path custom3", None, None, "custom3"),
    ],
)
def test_generator_manifest_path(
    tmp_path, monkeypatch, list_args, env, project_yml, expected
):
    if env:
        monkeypatch.setenv("DBT_TARGET_PATH", env)
    else:
        monkeypatch.delenv("DBT_TARGET_PATH", raising=False)
    if project_yml is not None:
        (tmp_path / "dbt_project.yml").write_text(project_yml)
    generator = BaseDbtGenerator.__new__(BaseDbtGenerator)
    generator.dbt_list_args = list_args
    assert generator.dbt_manifest_path(tmp_path) == (
        tmp_path / expected / "manifest.json"
    )


def test_generator_manifest_path_with_project_dir(tmp_path, monkeypatch):
    monkeypatch.delenv("DBT_TARGET_PATH", raising=False)
    (tmp_path / "transform").mkdir()
    (tmp_path / "transform" / "dbt_project.yml").write_text("target-path: out\n")
    generator = BaseDbtGenerator.__new__(BaseDbtGenerator)
    generator.dbt_list_args = "--project-dir transform"
    assert generator.dbt_manifest_path(tmp_path) == (
        tmp_path / "transform" / "out" / "manifest.json"
    )


def test_metadata_queries_json_columns_by_their_exact_name():
    # An ordinary (uppercase) Snowflake VARIANT column must be queried as
    # "PAYLOAD", not "payload"
    task = GenerateMetadataTask.__new__(GenerateMetadataTask)
    task.adapter = SimpleNamespace()
    task.NESTED_FIELD_TYPES = {"SimpleNamespace": "VARIANT"}
    columns = [
        SimpleNamespace(name="PAYLOAD", dtype="VARIANT"),
        SimpleNamespace(name="ID", dtype="NUMBER"),
    ]
    assert task.get_nested_columns(columns) == ["PAYLOAD"]


def test_generator_project_dir_from_env(tmp_path, monkeypatch):
    monkeypatch.delenv("DBT_TARGET_PATH", raising=False)
    monkeypatch.setenv("DBT_PROJECT_DIR", "transform")
    generator = BaseDbtGenerator.__new__(BaseDbtGenerator)
    generator.dbt_list_args = ""
    assert generator.dbt_manifest_path(tmp_path) == (
        tmp_path / "transform" / "target" / "manifest.json"
    )


def test_generator_warns_when_target_path_is_jinja(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("DBT_TARGET_PATH", raising=False)
    monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)
    (tmp_path / "dbt_project.yml").write_text("target-path: \"{{ var('t') }}\"\n")
    generator = BaseDbtGenerator.__new__(BaseDbtGenerator)
    generator.dbt_list_args = ""
    generator.dbt_manifest_path(tmp_path)
    assert "uses Jinja, assuming 'target'" in " ".join(capsys.readouterr().out.split())


def make_generator(tmp_path, readonly=True):
    from dbt_coves.tasks.generate.airflow_generators.base import BaseDbtGenerator

    generator = BaseDbtGenerator(dbt_project_path=str(tmp_path / "project"))
    generator.is_readonly = lambda folder: readonly
    return generator


def fake_subprocess(monkeypatch, fail_on=None):
    """Record commands; `git rev-parse` returns a commit; `fail_on` raises."""
    import subprocess

    from dbt_coves.tasks.generate.airflow_generators import base

    calls = []

    def run(cmd, **kwargs):
        calls.append(cmd)
        if fail_on and fail_on in cmd:
            raise subprocess.CalledProcessError(2, cmd, output=b"", stderr=b"boom")
        stdout = "abc123\n" if "rev-parse" in cmd else b""
        return SimpleNamespace(stdout=stdout, returncode=0)

    monkeypatch.setattr(base.subprocess, "run", run)
    return calls


@pytest.mark.parametrize("fail_on", ["deps", "ls"])
def test_readonly_copy_removed_when_dbt_fails(tmp_path, monkeypatch, fail_on):
    from dbt_coves.tasks.generate.airflow_generators.base import GeneratorException

    calls = fake_subprocess(monkeypatch, fail_on=fail_on)
    generator = make_generator(tmp_path)
    generator.run_dbt_deps = True
    with pytest.raises(GeneratorException):
        generator.discover_dbt_connections()
    assert ["rm", "-rf", "/tmp/airbyte-generator-abc123"] in calls


def test_corrupt_manifest_is_a_generator_error(tmp_path, monkeypatch):
    from dbt_coves.tasks.generate.airflow_generators.base import GeneratorException

    monkeypatch.delenv("DBT_TARGET_PATH", raising=False)
    monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)
    calls = fake_subprocess(monkeypatch)
    (tmp_path / "project" / "target").mkdir(parents=True)
    (tmp_path / "project" / "target" / "manifest.json").write_text('{"truncated":')
    generator = make_generator(tmp_path, readonly=False)
    with pytest.raises(GeneratorException, match="Couldn't read dbt's manifest.json"):
        generator.discover_dbt_connections()
    assert not any("rm" in c for c in calls)  # nothing was copied, nothing to remove
