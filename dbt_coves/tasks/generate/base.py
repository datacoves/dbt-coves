import re

from rich.console import Console

from dbt_coves.tasks.base import BaseConfiguredTask

console = Console()


class BaseGenerateTask(BaseConfiguredTask):
    """
    Provides common functionality for all "Generate" sub tasks.
    """

    arg_parser = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = None

    def get_schemas(self):

        # get schema names selectors
        schema_name_selectors = [
            schema.upper() for schema in self.get_config_value("schemas")
        ]

        schema_wildcard_selectors = []
        for schema_name in schema_name_selectors:
            if "*" in schema_name:
                schema_wildcard_selectors.append(schema_name.replace("*", ".*"))

        schemas = [
            schema.upper()
            for schema in self.adapter.list_schemas(self.db)
            # TODO: fix this for different adapters
            if schema != "INFORMATION_SCHEMA"
        ]

        for schema in schemas:
            for selector in schema_wildcard_selectors:
                if re.search(selector, schema):
                    schema_name_selectors.append(schema)
                    break

        filtered_schemas = list(set(schemas).intersection(schema_name_selectors))
        if not filtered_schemas:
            schema_nlg = f"schema{'s' if len(schema_name_selectors) > 1 else ''}"
            console.print(
                f"Provided {schema_nlg} [u]{', '.join(schema_name_selectors)}[/u] not found in Database.\n"
            )

            filtered_schemas = self.select_schemas(schemas)

        return filtered_schemas

    def select_schemas(self, schemas):
        return schemas

    def select_relations(self, rels):
        return rels

    def run(self) -> int:
        raise NotImplementedError()

    def render_templates(self, relation, columns, destination, nested=None):

        context = self.render_templates_get_context(relation, columns, nested)
        self.render_templates_render_context(context, destination)

    def render_templates_get_context(self, relation, columns, nested=None):
        context = {
            "relation": relation,
            "columns": columns,
            "nested": {},
            "adapter_name": self.adapter.__class__.__name__,
        }
        if nested:
            context["nested"] = self.get_nested_keys(
                nested, relation.schema, relation.name
            )
            # Removing original column with JSON data
            new_cols = []
            for col in columns:
                if col.name.lower() not in context["nested"]:
                    new_cols.append(col)
            context["columns"] = new_cols
        config_db = self.get_config_value("database")
        if config_db:
            context["source_database"] = config_db

        return context
