"""
Source and table names generate sources writes to sources.yml and passes to
source() in staging models (#649). They must match exactly (dbt's source() lookup
is case-sensitive), and on Snowflake identifiers created quoted (lowercase, mixed
case, ...) or named like reserved words need `quoting` to be found.
"""

from types import SimpleNamespace

import pytest
from ruamel.yaml import YAML

from dbt_coves.tasks.generate.sources import (
    GenerateSourcesTask,
    snowflake_source_name,
)
from dbt_coves.utils.jinja import get_render_output


@pytest.mark.parametrize(
    "identifier, expected",
    [
        ("RAW", ("raw", False)),
        ("MY_TABLE_1", ("my_table_1", False)),
        ("_PRIVATE$", ("_private$", False)),
        ("my_table", ("my_table", True)),
        ("MixedT", ("MixedT", True)),
        ("MY TABLE", ("MY TABLE", True)),
        ("MY-TABLE", ("MY-TABLE", True)),
        ("1ST", ("1ST", True)),
        # `$` would also match before a trailing newline; the whole name must match
        ("ORDERS\n", ("ORDERS\n", True)),
        # Reserved words keep their exact (uppercase) name and are quoted
        ("ORDER", ("ORDER", True)),
        ("TABLE", ("TABLE", True)),
        ("ROWS", ("ROWS", True)),
        ("ORDERS", ("orders", False)),
        # Keywords Snowflake only restricts elsewhere (SHOW commands, column
        # references, FROM aliases) work as qualified names: not quoted
        ("ACCOUNT", ("account", False)),
        ("ISSUE", ("issue", False)),
        ("CURRENT_DATE", ("current_date", False)),
        ("JOIN", ("join", False)),
    ],
)
def test_snowflake_source_name(identifier, expected):
    assert snowflake_source_name(identifier) == expected


def make_task(adapter_type):
    task = GenerateSourcesTask.__new__(GenerateSourcesTask)
    task.adapter = SimpleNamespace(type=lambda: adapter_type)
    return task


def render(template, adapter_type, schema, name, source_database=None):
    task = make_task(adapter_type)
    relation = SimpleNamespace(schema=schema, name=name)
    context = {
        "relation": relation,
        "source": task.get_source_context(relation),
        "columns": [],
        "nested": {},
        "adapter_name": "Other",
    }
    if source_database:
        context["source_database"] = source_database
    return get_render_output(template, context, templates_folder="no-such-folder")


SNOWFLAKE_CASES = [
    # (schema, table, source name, table name, quote schema, quote identifier)
    ("RAW", "UPPER_T", "raw", "upper_t", False, False),
    ("RAW", "lower_t", "raw", "lower_t", False, True),
    ("RAW", "MixedT", "raw", "MixedT", False, True),
    ("raw_lower", "t2", "raw_lower", "t2", True, True),
    ("RAW", "ORDER", "raw", "ORDER", False, True),
]


@pytest.mark.parametrize(
    "schema, table, name, table_name, q_schema, q_table", SNOWFLAKE_CASES
)
def test_snowflake_sources_yml_and_source_call_agree(
    schema, table, name, table_name, q_schema, q_table
):
    source = YAML(typ="safe").load(
        render("source_props.yml", "snowflake", schema, table, "DB")
    )["sources"][0]
    assert source["name"] == name
    assert source["database"] == "DB"
    assert source["tables"][0]["name"] == table_name
    assert source.get("quoting") == ({"schema": True} if q_schema else None)
    assert source["tables"][0].get("quoting") == (
        {"identifier": True} if q_table else None
    )
    sql = render("staging_model.sql", "snowflake", schema, table)
    assert f"source('{name}', '{table_name}')" in sql


# The template as it was before #649, to check other adapters are unchanged where
# that output worked.
OLD_SOURCE_PROPS = """version: 2

sources:
  - name: {% if not relation.schema.isupper() and not relation.schema.islower() %}{{ relation.schema }}{% else %}{{ relation.schema.lower() }}{% endif %}
{%- if source_database %}
    database: {{ source_database }}
{%- endif %}
    tables:
      - name: {% if not relation.name.isupper() and not relation.name.islower() %}{{ relation.name }}{% else %}{{ relation.name.lower() }}{% endif %}
        description: ""
"""  # noqa: E501


@pytest.mark.parametrize("adapter_type", ["redshift", "bigquery"])
@pytest.mark.parametrize(
    "schema, table",
    [("raw", "orders"), ("MyDataset", "MyTable"), ("raw", "events_2026")],
)
def test_other_adapters_unchanged_for_lowercase_and_mixed_case(
    adapter_type, schema, table, tmp_path
):
    # Render the old template through the same renderer, from a templates folder
    # (which takes precedence over the packaged templates).
    (tmp_path / "source_props.yml").write_text(OLD_SOURCE_PROPS)
    relation = SimpleNamespace(schema=schema, name=table)
    old = get_render_output(
        "source_props.yml",
        {"relation": relation, "source_database": "DB"},
        templates_folder=str(tmp_path),
    )
    assert render("source_props.yml", adapter_type, schema, table, "DB") == old
    sql = render("staging_model.sql", adapter_type, schema, table)
    assert f"source('{schema}', '{table}')" in sql


@pytest.mark.parametrize("adapter_type", ["redshift", "bigquery"])
def test_other_adapters_use_exact_names_in_both_places(adapter_type):
    # Before #649 sources.yml lowercased these while source() kept them uppercase,
    # so dbt couldn't find the source.
    source = YAML(typ="safe").load(
        render("source_props.yml", adapter_type, "RAW_DATA", "EVENTS")
    )["sources"][0]
    assert (source["name"], source["tables"][0]["name"]) == ("RAW_DATA", "EVENTS")
    assert "quoting" not in source and "quoting" not in source["tables"][0]
    sql = render("staging_model.sql", adapter_type, "RAW_DATA", "EVENTS")
    assert "source('RAW_DATA', 'EVENTS')" in sql


def test_update_merges_quoting_into_existing_sources_yml():
    task = make_task("snowflake")
    # Quoting the user set by hand is kept.
    current = {
        "name": "raw",
        "database": "DB",
        "quoting": {"database": True},
        "tables": [{"name": "lower_t", "quoting": {"database": True}}],
    }
    new = {
        "name": "raw",
        "database": "DB",
        "quoting": {"schema": True},
        "tables": [{"name": "lower_t", "quoting": {"identifier": True}}],
    }
    merged = task.update_source_properties(current, new)
    assert merged["quoting"] == {"database": True, "schema": True}
    assert merged["tables"][0]["quoting"] == {"database": True, "identifier": True}


def test_snowflake_json_columns_query_is_quoted(monkeypatch):
    task = make_task("snowflake")
    task.adapter.quote = lambda name: f'"{name}"'
    executed = []

    def execute(sql, fetch):
        executed.append(sql)
        return None, SimpleNamespace(rows=[])

    task.adapter.execute = execute
    task.get_config_value = lambda key: None
    task.get_nested_keys(["payload"], SimpleNamespace(schema="RAW", name="json_t"))
    assert executed == ['SELECT "payload" FROM "RAW"."json_t" limit 1']
