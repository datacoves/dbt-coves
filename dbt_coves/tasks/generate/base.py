import csv
import re
from pathlib import Path

import questionary
import yaml
from rich.console import Console
from slugify import slugify

from dbt_coves.tasks.base import BaseConfiguredTask
from dbt_coves.utils.jinja import get_render_output, render_template_file
from dbt_coves.utils.yaml import open_yaml, save_yaml

console = Console()


class BaseGenerateTask(BaseConfiguredTask):
    """
    Provides common functionality for all "Generate" sub tasks.
    """

    arg_parser = None
    NESTED_FIELD_TYPES = {
        "SnowflakeAdapter": "VARIANT",
        "BigQueryAdapter": "STRUCT",
        "RedshiftAdapter": "SUPER",
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.metadata = None
        self.prop_files_created_by_dbtcoves = set()

    def get_schemas(self):
        # get schema names selectors
        schema_name_selectors = [schema for schema in self.get_config_value("schemas")]

        schema_wildcard_selectors = []
        for schema_name in schema_name_selectors:
            if "*" in schema_name:
                schema_wildcard_selectors.append(schema_name.replace("*", ".*"))

        schemas = [
            schema
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
            if not filtered_schemas:
                console.print(f"No schemas selected")
                exit()
        return filtered_schemas

    def select_schemas(self, schemas):
        selected_schemas = questionary.checkbox(
            "Which schemas would you like to inspect?",
            choices=schemas,
        ).ask()

        return selected_schemas

    def get_relations(self, filtered_schemas):
        rel_name_selectors = [
            relation for relation in self.get_config_value("relations")
        ]
        rel_wildcard_selectors = []
        for rel_name in rel_name_selectors:
            if "*" in rel_name:
                rel_wildcard_selectors.append(rel_name.replace("*", ".*"))

        listed_relations = []
        for schema in filtered_schemas:
            listed_relations += self.adapter.list_relations(self.db, schema)

        for rel in listed_relations:
            for selector in rel_wildcard_selectors:
                if re.search(selector, rel.name):
                    rel_name_selectors.append(rel.name)
                    break

        intersected_rels = [
            relation
            for relation in listed_relations
            if relation.name in rel_name_selectors
        ]
        rels = (
            intersected_rels
            if rel_name_selectors and rel_name_selectors[0]
            else listed_relations
        )

        return rels

    def run(self) -> int:
        raise NotImplementedError()

    def get_metadata_map_key(self, row):
        map_key = f"{row['database'].lower()}-{row['schema'].lower()}-{row['relation'].lower()}-{row['column'].lower()}-{row.get('key', '').lower()}"
        return map_key

    def get_metadata_map_item(self, row):
        if row["description"] is None:
            row["description"] = ""
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
                            raise Exception(
                                f"Key {e} not found in {path}. Please check this sample metadata file: https://raw.githubusercontent.com/datacoves/dbt-coves/main/sample_metadata.csv."
                            )
            except FileNotFoundError as e:
                raise Exception(f"Metadata file not found: {e}")

        self.metadata = metadata_map

        return metadata_map

    def get_config_value(self, key):
        return self.coves_config.integrated["generate"][self.args.task][key]

    def render_templates(
        self, relation, columns, destination, options=None, json_cols=None
    ):
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
        resource_type,
    ):
        new_yml = yaml.safe_load(
            get_render_output(
                template,
                context,
                templates_folder=templates_folder,
            )
        )
        resource_type_key = f"{resource_type}s"
        for new_obj in new_yml.get(resource_type_key):
            for curr_obj in current_yml.get(resource_type_key):
                if curr_obj.get("name") == new_obj.get("name"):
                    return new_obj
        return False

    def create_property_file(self, template, context, yml_path, templates_folder):
        self.render_property_file(template, context, yml_path, templates_folder)
        self.prop_files_created_by_dbtcoves.add(yml_path)
        console.print(f"Property file [green][b]{yml_path}[/b][/green] created")

    def render_property_files(
        self,
        context,
        options,
        templates_folder,
        update_strategy,
        resource_type,
        yml_path,
        template,
    ):
        strategy_key_update_all = ""
        strategy_key_recreate_all = ""
        rel = context["relation"]

        context["model"] = rel.name
        strategy_key_update_all = f"{resource_type}_prop_update_all"
        strategy_key_recreate_all = f"{resource_type}_prop_recreate_all"
        if yml_path.exists():
            object_in_yml = False
            current_yml = open_yaml(yml_path)
            if not current_yml:
                # target yml path exists but it's empty -> recreate file
                return self.create_property_file(
                    template, context, yml_path, templates_folder
                )
            object_in_yml = self.new_object_exists_in_current_yml(
                current_yml,
                template,
                context,
                templates_folder,
                resource_type,
            )
            sel_action = None
            if object_in_yml:
                new_object_id = object_in_yml.get("name")
                if (
                    not options[strategy_key_recreate_all]
                    and not options[strategy_key_update_all]
                    and yml_path not in self.prop_files_created_by_dbtcoves
                ):
                    if update_strategy == "ask":
                        console.print(
                            f"{resource_type} [yellow][b]{new_object_id}[/b][/yellow] already exists in [b][yellow]{yml_path}[/b][/yellow]."
                        )
                        action = questionary.select(
                            "What would you like to do with it?",
                            choices=[
                                "Update",
                                "Update all",
                                "Recreate",
                                "Recreate all",
                                "Skip",
                                "Cancel",
                            ],
                        ).ask()
                        if action == "Recreate":
                            sel_action = "recreate"
                        elif action == "Recreate all":
                            options[strategy_key_recreate_all] = True
                            sel_action = "recreate"
                        elif action == "Update":
                            sel_action = "update"
                        elif action == "Update all":
                            options[strategy_key_update_all] = True
                            sel_action = "update"
                        elif action == "Skip":
                            return
                        elif action == "Cancel":
                            exit()
                    elif update_strategy == "update":
                        sel_action = "update"
                    elif update_strategy == "recreate":
                        sel_action = "recreate"
                    else:
                        console.print(
                            f"Update strategy {update_strategy} not a valid option."
                        )
                        exit()
                elif options[strategy_key_recreate_all]:
                    sel_action = "recreate"
                elif (
                    options[strategy_key_update_all]
                    or yml_path in self.prop_files_created_by_dbtcoves
                ):
                    sel_action = "update"
            else:
                sel_action = "create"
            self.modify_property_file(
                template,
                context,
                yml_path,
                current_yml,
                templates_folder,
                resource_type,
                sel_action,
            )
        else:
            self.create_property_file(template, context, yml_path, templates_folder)

    def update_object_properties(self, current_object, new_object, resource_type):
        if resource_type == "source":
            current_object = self.update_source_properties(current_object, new_object)
        if resource_type == "model":
            current_object = self.update_model_properties(current_object, new_object)
        return current_object

    def modify_property_file(
        self,
        template,
        context,
        yml_path,
        current_yml,
        templates_folder,
        resource_type,
        action,
    ):
        new_yml = yaml.safe_load(
            get_render_output(
                template,
                context,
                templates_folder=templates_folder,
            )
        )
        resource_type_key = resource_type + "s"
        new_object = new_yml.get(resource_type_key)[0]

        if action == "create":
            current_yml[resource_type_key].append(new_object)
        elif action == "recreate" or action == "update":
            for idx, curr_obj in enumerate(current_yml.get(resource_type_key)):
                if curr_obj.get("name") == new_object.get("name"):
                    if action == "recreate":
                        current_yml[resource_type_key][idx] = new_object
                    if action == "update":
                        current_yml[resource_type_key][
                            idx
                        ] = self.update_object_properties(
                            curr_obj, new_object, resource_type
                        )

        # "{Model/Source} {name} created/recreated/updated on file {filepath}"
        console.print(
            f"{resource_type.capitalize()} [green][b]{new_object.get('name')}[/b][/green] {action}d on file [green][b]{yml_path}[/b][/green]"
        )

        save_yaml(yml_path, current_yml)

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
                    if (
                        current_column.get("name") == new_column.get("name")
                    ) and new_column.get("description"):
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
