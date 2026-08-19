# Column alias generation tests
#
# Aliases are slugified, and slugify strips leading/trailing separators, so a
# column named `_2010` used to render as `as 2010` -- a numeric literal instead
# of an identifier (issue #629).

from types import SimpleNamespace

import pytest

from dbt_coves.tasks.generate.base import BaseGenerateTask
from dbt_coves.utils.jinja import get_render_output, render_template


class SnowflakeAdapter:
    pass


class RedshiftAdapter:
    pass


class BigQueryAdapter:
    pass


class Task(BaseGenerateTask):
    # The task's config isn't needed to build aliases, only its adapter
    def __init__(self, adapter):
        self.adapter = adapter()


def get_id(name, adapter=SnowflakeAdapter):
    return Task(adapter).get_column_id(name)


def get_column(name, adapter=SnowflakeAdapter):
    return Task(adapter).get_default_metadata_item(name, type="varchar")


def render(template, context):
    # No templates_folder is set up, so the packaged templates are used
    return get_render_output(template, context, templates_folder="nonexistent")


@pytest.mark.parametrize(
    "name, id",
    [
        ("STATES", "states"),
        ("Order Date", "order_date"),
        ("already_slugged", "already_slugged"),
    ],
)
def test_valid_aliases_are_not_quoted(name, id):
    assert get_id(name) == id


@pytest.mark.parametrize("adapter", [SnowflakeAdapter, RedshiftAdapter])
def test_leading_digit_alias_is_quoted(adapter):
    assert get_id("_2010", adapter=adapter) == '"2010"'


def test_leading_digit_alias_is_prefixed_on_bigquery():
    # BigQuery rejects column names not starting with a letter or an underscore
    # even when they are quoted
    assert get_id("_2010", adapter=BigQueryAdapter) == "_2010"


def test_alias_falls_back_to_the_column_name_when_slugify_empties_it():
    assert get_id("$") == '"$"'


def test_default_metadata_item_carries_the_id():
    assert get_column("_2010") == {
        "name": "_2010",
        "id": '"2010"',
        "type": "varchar",
        "description": "",
        "numeric_precision": None,
        "numeric_scale": None,
    }


def test_staging_model_quotes_the_alias():
    context = {
        "relation": SimpleNamespace(schema="RAW", name="US_POPULATION"),
        "columns": [get_column("STATES"), get_column("_2010")],
        "nested": {},
        "adapter_name": "SnowflakeAdapter",
    }
    output = render("staging_model.sql", context)
    assert '"STATES"::varchar as states,' in output
    assert '"_2010"::varchar as "2010"' in output


def test_model_props_quotes_the_column_name():
    context = {
        "model": "US_POPULATION",
        "columns": [get_column("STATES"), get_column("_2010")],
    }
    output = render("model_props.yml", context)
    assert "      - name: states" in output
    assert '      - name: "2010"' in output


def test_quoting_reaches_templates_kept_by_the_project():
    # Projects keep their own copies of the templates under `.dbt_coves/templates`,
    # and those take precedence over the packaged ones, so the quoting has to hold
    # for a template that only knows about `id`
    context = {"columns": [get_column("STATES"), get_column("_2010")]}
    sql = render_template(
        "{% for col in columns %}as {{ col['id'] }}\n{% endfor %}", context
    )
    assert sql == 'as states\nas "2010"\n'
