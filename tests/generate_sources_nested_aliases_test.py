"""
Flattened JSON keys were aliased from the key alone, so RAW.USGS__EARTHQUAKE_DATA.
EARTHQUAKES (PROPERTIES:type, GEOMETRY:type and a TYPE column) got three `as type`
columns and Snowflake refused the model: "duplicate column name 'TYPE'".
"""

import re

from dbt_coves.tasks.generate.sources import GenerateSourcesTask
from dbt_coves.utils.jinja import get_render_output


class SnowflakeAdapter:
    pass


class Task(GenerateSourcesTask):
    def __init__(self):
        self.adapter = SnowflakeAdapter()


def context(columns, nested):
    task = Task()
    ctx = {
        "relation": None,
        "source": {"name": "usgs", "table": "earthquakes"},
        "columns": [task.get_default_metadata_item(c) for c in columns],
        "nested": {
            json_col: {k: task.get_default_metadata_item(k) for k in keys}
            for json_col, keys in nested.items()
        },
        "adapter_name": "SnowflakeAdapter",
    }
    task.dedupe_nested_aliases(ctx)
    return ctx


def aliases(ctx):
    sql = get_render_output("staging_model.sql", ctx, templates_folder="nonexistent")
    # Only the select list's `<expr>::<type> as <alias>` lines, not the CTEs
    return re.findall(r"::\S+ as (\S+?),?$", sql, flags=re.MULTILINE)


def test_earthquakes_aliases_are_unique():
    ctx = context(
        ["ID", "PROPERTIES", "GEOMETRY", "TYPE"],
        {"PROPERTIES": ["mag", "type"], "GEOMETRY": ["coordinates", "type"]},
    )
    found = aliases(ctx)
    assert len(found) == len(set(found))
    assert sorted(found) == sorted(
        [
            "mag",
            "properties_type",
            "coordinates",
            "geometry_type",
            "id",
            "properties",
            "geometry",
            "type",
        ]
    )


def test_keys_that_dont_clash_keep_their_alias():
    ctx = context(["ID", "DATA"], {"DATA": ["name", "Player Name"]})
    assert aliases(ctx) == ["name", "player_name", "id", "data"]


def test_key_clashing_with_another_key_only():
    ctx = context(["ID"], {"A": ["x"], "B": ["x"]})
    assert aliases(ctx) == ["a_x", "b_x", "id"]


def test_prefixed_alias_that_is_taken_gets_a_number():
    # A real column already named properties_type
    ctx = context(
        ["TYPE", "PROPERTIES_TYPE"],
        {"PROPERTIES": ["type"]},
    )
    found = aliases(ctx)
    assert "properties_type_2" in found
    assert len(found) == len(set(found))


def test_clashes_ignore_case_and_quoting():
    # "Type" and TYPE are both aliased type
    ctx = context(["TYPE"], {"DATA": ["Type"]})
    assert aliases(ctx) == ["data_type", "type"]
