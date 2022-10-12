import csv
import re
from pathlib import Path

import questionary
import yaml
from rich.console import Console
from slugify import slugify

from dbt_coves.tasks.base import BaseConfiguredTask
from dbt_coves.utils.jinja import (
    get_render_output,
    render_template,
    render_template_file,
)

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
        schema_name_selectors = [schema.upper() for schema in self.get_config_value("schemas")]

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
        return {
            "name": name,
            "id": slugify(name, separator="_"),
            "type": type,
            "description": description,
        }

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
                            raise Exception(f"Key {e} not found in metadata file {path}")
            except FileNotFoundError as e:
                raise Exception(f"Metadata file not found: {e}")

        self.metadata = metadata_map

        return metadata_map

    def get_config_value(self, key):
        return self.coves_config.integrated["generate"][self.args.task][key]

    def generate_template(self, destination_path, relation):
        template_context = dict()
        if "{{schema}}" in destination_path.replace(" ", ""):
            template_context["schema"] = relation.schema.lower()
        if "{{relation}}" in destination_path.replace(" ", ""):
            template_context["relation"] = relation.name.lower()
        return render_template(destination_path, template_context)

    def render_templates(self, relation, columns, destination, options=None, json_cols=None):
        destination.parent.mkdir(parents=True, exist_ok=True)
        context = self.get_templates_context(relation, columns, json_cols)
        self.render_templates_with_context(context, destination, options)

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
                    "column": col.name,
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

    def new_object_exists_in_current_yml(
        self,
        current_yml,
        template,
        context,
        templates_folder,
        object_type,
    ):
        new_yml = yaml.safe_load(
            get_render_output(
                template,
                context,
                templates_folder=templates_folder,
            )
        )

        for new_obj in new_yml.get(object_type):
            for curr_obj in current_yml.get(object_type):
                if curr_obj.get("name") == new_obj.get("name"):
                    return new_obj
        return False

    def render_property_files(
        self,
        context,
        options,
        templates_folder,
        update_strategy,
        object_type,
        destination=None,
        template=None,
    ):
        strategy_key_update_all = ""
        strategy_key_recreate_all = ""

        rel = context["relation"]
        yml_dest = self.generate_template(destination, rel)
        yml_path = Path().joinpath(yml_dest)

        context["model"] = rel.name.lower()
        if object_type == "models":
            strategy_key_update_all = "model_prop_update_all"
            strategy_key_recreate_all = "model_prop_recreate_all"
        if object_type == "sources":
            strategy_key_update_all = "source_prop_update_all"
            strategy_key_recreate_all = "source_prop_recreate_all"
        if yml_path.exists():
            object_in_yml = False
            with open(yml_path, "r") as file:
                current_yml = yaml.safe_load(file)
                object_in_yml = self.new_object_exists_in_current_yml(
                    current_yml,
                    template,
                    context,
                    templates_folder,
                    object_type,
                )
            if object_in_yml:
                new_object_id = object_in_yml.get("name")
                if not options[strategy_key_recreate_all] and not options[strategy_key_update_all]:
                    if update_strategy == "ask":
                        action = questionary.select(
                            f"{object_type} {new_object_id} already exists in {yml_path}. What would you like to do with it?",
                            choices=[
                                "Update",
                                "Update all",
                                "Recreate",
                                "Recreate all",
                                "Skip",
                                "Cancel",
                            ],
                            default="Update",
                        ).ask()
                        if action == "Recreate":
                            self.modify_property_file(
                                template,
                                context,
                                yml_path,
                                current_yml,
                                templates_folder,
                                object_type,
                                "recreate",
                            )
                        if action == "Recreate all":
                            options[strategy_key_recreate_all] = True
                            self.modify_property_file(
                                template,
                                context,
                                yml_path,
                                current_yml,
                                templates_folder,
                                object_type,
                                "recreate",
                            )
                        if action == "Update":
                            self.modify_property_file(
                                template,
                                context,
                                yml_path,
                                current_yml,
                                templates_folder,
                                object_type,
                                "update",
                            )
                        if action == "Update all":
                            options[strategy_key_update_all] = True
                            self.modify_property_file(
                                template,
                                context,
                                yml_path,
                                current_yml,
                                templates_folder,
                                object_type,
                                "update",
                            )
                        if action == "Skip":
                            pass
                        if action == "Cancel":
                            exit
                    elif update_strategy == "update":
                        self.modify_property_file(
                            template,
                            context,
                            yml_path,
                            current_yml,
                            templates_folder,
                            object_type,
                            "update",
                        )
                    elif update_strategy == "recreate":
                        self.modify_property_file(
                            template,
                            context,
                            yml_path,
                            current_yml,
                            templates_folder,
                            object_type,
                            "recreate",
                        )
                    else:
                        exit
                if options[strategy_key_recreate_all]:
                    self.modify_property_file(
                        template,
                        context,
                        yml_path,
                        current_yml,
                        templates_folder,
                        object_type,
                        "recreate",
                    )
                if options[strategy_key_update_all]:
                    self.modify_property_file(
                        template,
                        context,
                        yml_path,
                        current_yml,
                        templates_folder,
                        object_type,
                        "update",
                    )
            else:
                self.modify_property_file(
                    template,
                    context,
                    yml_path,
                    current_yml,
                    templates_folder,
                    object_type,
                    "create",
                )
        else:
            self.render_property_file(template, context, yml_path, templates_folder)

    def update_object_properties(self, current_object, new_object, object_type):
        if object_type == "sources":
            current_object = self.update_source_properties(current_object, new_object)
        if object_type == "models":
            current_object = self.update_model_properties(current_object, new_object)
        return current_object

    def modify_property_file(
        self,
        template,
        context,
        yml_path,
        current_yml,
        templates_folder,
        obj_type,
        action,
    ):
        new_yml = yaml.safe_load(
            get_render_output(
                template,
                context,
                templates_folder=templates_folder,
            )
        )
        new_object = new_yml.get(obj_type)[0]

        if action == "create":
            current_yml[obj_type].append(new_object)
        if action == "recreate" or action == "update":
            for idx, curr_obj in enumerate(current_yml.get(obj_type)):
                if curr_obj.get("name") == new_object.get("name"):
                    if action == "recreate":
                        current_yml[obj_type][idx] = new_object
                    if action == "update":
                        current_yml[obj_type][idx] = self.update_object_properties(
                            curr_obj, new_object, obj_type
                        )

        with open(yml_path, "w") as file:
            yaml.safe_dump(current_yml, file, sort_keys=False)

    def render_property_file(self, template, context, model_yml, templates_folder):
        model_yml.parent.mkdir(parents=True, exist_ok=True)
        render_template_file(
            template,
            context,
            model_yml,
            templates_folder=templates_folder,
        )

    def update_model_columns(self, columns_a: list, columns_b: list):
        model_a_column_names = [col.get("name") for col in columns_a]
        for new_column in columns_b:
            if new_column.get("name") in model_a_column_names:
                # If column exists in A, update it's description
                # and leave as-is to avoid overriding tests
                for current_column in columns_a:
                    if (current_column.get("name") == new_column.get("name")) and new_column.get(
                        "description"
                    ):
                        current_column["description"] = new_column.get("description")
            else:
                columns_a.append(new_column)

    def update_model_properties(self, model_a: dict, model_b: dict):
        if model_b.get("description"):
            model_a["description"] = model_b.get("description")
        self.update_model_columns(model_a.get("columns"), model_b.get("columns"))
        return model_a

    def update_source_tables(self, tables_a: list, tables_b: list):
        source_a_table_names = [table.get("name") for table in tables_a]
        for new_table in tables_b:
            if new_table.get("name") in source_a_table_names:
                # If table exists in A, update it's description and identifier
                # and leave as-is to avoid overriding tests
                for current_table in tables_a:
                    if current_table.get("name") == new_table.get("name"):
                        if new_table.get("description"):
                            current_table["description"] = new_table.get("description")
                        if new_table.get("identifier"):
                            current_table["identifier"] = new_table.get("identifier")
            else:
                tables_a.append(new_table)

    def update_source_properties(self, source_a: dict, source_b: dict):
        source_a["database"] = source_b.get("database")
        if source_b.get("schema"):
            source_a["schema"] = source_b.get("schema")
        self.update_source_tables(source_a.get("tables"), source_b.get("tables"))
        return source_a
