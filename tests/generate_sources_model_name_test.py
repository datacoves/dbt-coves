"""
The built-in staging_model_props.yml named the model after the relation, but dbt names
a model after its file. With models_destination `stg_{{relation}}.sql` (Balboa's) the
properties described `golf` for stg_golf.sql, and dbt warned "Did not find matching node
for patch with name 'golf'".
"""

from types import SimpleNamespace

from ruamel.yaml import YAML

from dbt_coves.tasks.generate.sources import GenerateSourcesTask

CONFIG = {
    "skip_model_props": False,
    "templates_folder": "nonexistent",  # use the built-in templates
    "update_strategy": "update",
    "models_destination": "models/{{schema}}/stg_{{relation}}.sql",
    "model_props_destination": "models/{{schema}}/stg_{{relation}}.yml",
    "sources_destination": "models/{{schema}}/_{{schema}}.yml",
}


class SnowflakeAdapter:
    @staticmethod
    def type():
        return "snowflake"


class Task(GenerateSourcesTask):
    def __init__(self, project_root):
        self.adapter = SnowflakeAdapter()
        self.config = SimpleNamespace(project_root=str(project_root))
        self.no_prompt = True
        self.prop_files_created_by_dbtcoves = set()

    def get_config_value(self, key):
        return CONFIG[key]


def generate(tmp_path):
    task = Task(tmp_path)
    relation = SimpleNamespace(database="RAW", schema="RAW", name="GOLF")
    context = {
        "relation": relation,
        "source": task.get_source_context(relation),
        "columns": [task.get_default_metadata_item("PLAYER")],
        "nested": {},
        "adapter_name": "SnowflakeAdapter",
    }
    sql = tmp_path / task.render_path_template(CONFIG["models_destination"], relation)
    sql.parent.mkdir(parents=True, exist_ok=True)
    options = {
        "model_prop_update_all": True,
        "model_prop_recreate_all": False,
        "source_prop_update_all": True,
        "source_prop_recreate_all": False,
    }
    task.render_templates_with_context(context, sql, options)
    return sql, YAML().load(sql.with_suffix(".yml"))


def test_props_name_the_model_after_its_file(tmp_path):
    sql, props = generate(tmp_path)
    assert sql.name == "stg_golf.sql"
    assert [m["name"] for m in props["models"]] == ["stg_golf"]


def test_regenerating_updates_the_same_model(tmp_path):
    generate(tmp_path)
    _, props = generate(tmp_path)
    assert [m["name"] for m in props["models"]] == ["stg_golf"]
