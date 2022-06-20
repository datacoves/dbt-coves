import re
import csv
from pathlib import Path
from slugify import slugify

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
        self.metadata = None

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

    def get_metadata_map_key(self, row):
        map_key = f"{row['database'].lower()}-{row['schema'].lower()}-{row['relation'].lower()}-{row['column'].lower()}-{row.get('key', '').lower()}"
        return map_key

    def get_metadata_map_item(self, row):
        data = {
            "type": row["type"],
            "description": row["description"].strip(),
        }
        return data

    def get_default_metadata_item(self, name, type="varchar", description=""):
        return {"name": name, "id": slugify(name, separator="_"), "type": type, "description": description}

    def get_metadata(self):
        """
        If metadata path is configured, returns a dictionary with column keys and their corresponding values.
        If metadata is already set, do not load again and return the existing value.
        """
        path = self.get_config_value("metadata")

        if self.metadata:
            return self.metadata

        metadata_map = dict()
        if path:
            metadata_path = Path().joinpath(path)
            try:
                with open(metadata_path, "r") as csvfile:
                    rows = csv.DictReader(csvfile, skipinitialspace=True)
                    for row in rows:
                        try:
                            metadata_map[
                                self.get_metadata_map_key(row)
                            ] = self.get_metadata_map_item(row)
                        except KeyError as e:
                            raise Exception(
                                f"Key {e} not found in metadata file {path}"
                            )
            except FileNotFoundError as e:
                raise Exception(f"Metadata file not found: {e}")

        self.metadata = metadata_map

        return metadata_map

    def render_templates(self, relation, columns, destination, json_cols=None):

        context = self.get_templates_context(relation, columns, json_cols)
        self.render_templates_with_context(context, destination)

    def get_templates_context(self, relation, columns, json_cols=None):
        return {
            "relation": relation,
            "columns": self.get_metadata_columns(relation, columns),
            "nested": {},
            "adapter_name": self.adapter.__class__.__name__,
        }

    def get_metadata_columns(self, relation, cols):
        """
        Get metadata col
        """
        metadata = self.get_metadata()
        metadata_cols = []
        for col in cols:
            new_col = None
            if metadata:
                metadata_map_key_data = {
                    "database": relation.database,
                    "schema": relation.schema,
                    "relation": relation.name,
                    "column": col.name
                }
                metadata_key = self.get_metadata_map_key(metadata_map_key_data)
                new_col = metadata.get(metadata_key)
                if new_col:
                    # FIXME: DRY this
                    new_col["name"] = col.name
                    new_col["id"] = slugify(col.name, separator="_")
            if not new_col:
                new_col = self.get_default_metadata_item(col.name, type=col.dtype)
            metadata_cols.append(new_col)
        return metadata_cols