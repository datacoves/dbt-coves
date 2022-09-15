from __future__ import nested_scopes
from collections import OrderedDict
import copy
import json
import re
import yaml
from pathlib import Path

import questionary

from questionary import Choice
from rich.console import Console


from dbt_coves.utils.jinja import render_template, render_template_file

from .base import BaseGenerateTask

console = Console()


NESTED_FIELD_TYPES = {
    "SnowflakeAdapter": "VARIANT",
    "BigQueryAdapter": "STRUCT",
    "RedshiftAdapter": "SUPER",
}


class GenerateSourcesTask(BaseGenerateTask):
    """
    Task that generate sources, models and model properties automatically
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "sources",
            parents=[base_subparser],
            help="Generate source dbt models by inspecting the database schemas and relations.",
        )
        subparser.add_argument(
            "--database",
            type=str,
            help="Database where source relations live, if different than target",
        )
        subparser.add_argument(
            "--schemas",
            type=str,
            help="Comma separated list of schemas where raw data resides, "
            "i.e. 'RAW_SALESFORCE,RAW_HUBSPOT'",
        )
        subparser.add_argument(
            "--relations",
            type=str,
            help="Comma separated list of relations where raw data resides, "
            "i.e. 'RAW_HUBSPOT_PRODUCTS,RAW_SALESFORCE_USERS'",
        )
        subparser.add_argument(
            "--sources-destination",
            type=str,
            help="Where sources yml files will be generated, i.e. "
            "'models/sources/{{schema}}/{{relation}}.yml'",
        )
        subparser.add_argument(
            "--models-destination",
            type=str,
            help="Where models sql files will be generated, i.e. "
            "'models/staging/{{schema}}/{{relation}}.sql'",
        )
        subparser.add_argument(
            "--model-props-destination",
            type=str,
            help="Where models yml files will be generated, i.e. "
            "'models/staging/{{schema}}/{{relation}}.yml'",
        )
        subparser.add_argument(
            "--update-strategy",
            type=str,
            help="Action to perform when a property file already exists"
            "'update', 'recreate', 'fail', 'ask' (per file)",
        )
        subparser.add_argument(
            "--templates-folder",
            type=str,
            help="Folder with jinja templates that override default "
            "sources generation templates, i.e. 'templates'",
        )
        subparser.add_argument(
            "--metadata",
            type=str,
            help="Path to csv file containing metadata, i.e. 'metadata.csv'",
        )
        cls.arg_parser = base_subparser
        subparser.set_defaults(cls=cls, which="sources")
        return subparser

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = None

    def get_config_value(self, key):
        return self.coves_config.integrated["generate"]["sources"][key]

    def select_schemas(self, schemas):
        selected_schemas = questionary.checkbox(
            "Which schemas would you like to inspect?",
            choices=[
                Choice(schema, checked=True) if "RAW" in schema else Choice(schema)
                for schema in schemas
            ],
        ).ask()

        return selected_schemas

    def get_relations(self, filtered_schemas):
        rel_name_selectors = [
            relation.upper() for relation in self.get_config_value("relations")
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

    def select_relations(self, rels):
        selected_rels = questionary.checkbox(
            "Which sources would you like to generate?",
            choices=[
                Choice(f"[{rel.schema}] {rel.name}", checked=True, value=rel)
                for rel in rels
            ],
        ).ask()

        return selected_rels

    def generate_template(self, destination_path, relation):
        template_context = dict()
        if "{{schema}}" in destination_path.replace(" ", ""):
            template_context["schema"] = relation.schema.lower()
        if "{{relation}}" in destination_path.replace(" ", ""):
            template_context["relation"] = relation.name.lower()
        return render_template(destination_path, template_context)

    def generate(self, rels):
        models_destination = self.get_config_value("models_destination")
        options = {
            "override_all": None,
            "flatten_all": None,
            "model_prop_is_single_file": False,
            "model_prop_update_all": False,
            "model_prop_recreate_all": False,
            "source_prop_is_single_file": False,
            "source_prop_update_all": False,
            "source_prop_recreate_all": False,
        }
        for rel in rels:
            model_dest = self.generate_template(models_destination, rel)
            model_sql = Path().joinpath(model_dest)
            if not options["override_all"]:
                if model_sql.exists():
                    overwrite = questionary.select(
                        f"{model_dest} already exists. Would you like to overwrite it?",
                        choices=["No", "Yes", "No for all", "Yes for all"],
                        default="No",
                    ).ask()
                    if overwrite == "Yes":
                        self.generate_model(
                            rel,
                            model_sql,
                            options,
                        )
                    elif overwrite == "No for all":
                        options["override_all"] = "No"
                    elif overwrite == "Yes for all":
                        options["override_all"] = "Yes"
                        self.generate_model(
                            rel,
                            model_sql,
                            options,
                        )
                else:
                    self.generate_model(
                        rel,
                        model_sql,
                        options,
                    )
            elif options["override_all"] == "Yes":
                self.generate_model(rel, model_sql, options)
            else:
                if not model_sql.exists():
                    self.generate_model(
                        rel,
                        model_sql,
                        options,
                    )

    def generate_model(self, relation, destination, options):
        destination.parent.mkdir(parents=True, exist_ok=True)
        columns = self.adapter.get_columns_in_relation(relation)
        nested_field_type = NESTED_FIELD_TYPES.get(self.adapter.__class__.__name__)
        nested = [col.name.lower() for col in columns if col.dtype == nested_field_type]
        if not options["flatten_all"]:
            if nested:
                field_nlg = "field"
                flatten_nlg = "flatten it"
                if len(nested) > 1:
                    field_nlg = "fields"
                    flatten_nlg = "flatten them"
                flatten = questionary.select(
                    f"{relation.name.lower()} contains the JSON {field_nlg} {', '.join(nested)}."
                    f" Would you like to {flatten_nlg}?",
                    choices=["No", "Yes", "No for all", "Yes for all"],
                    default="Yes",
                ).ask()
                if flatten == "Yes":
                    self.render_templates(
                        relation,
                        columns,
                        destination,
                        options,
                        json_cols=nested,
                    )
                elif flatten == "No":
                    self.render_templates(relation, columns, destination, options)
                elif flatten == "No for all":
                    options["flatten_all"] = "No"
                    self.render_templates(relation, columns, destination, options)
                elif flatten == "Yes for all":
                    options["flatten_all"] = "Yes"
                    self.render_templates(
                        relation,
                        columns,
                        destination,
                        options,
                        json_cols=nested,
                    )
            else:
                self.render_templates(relation, columns, destination, options)
        elif options["flatten_all"] == "Yes":
            if nested:
                self.render_templates(
                    relation,
                    columns,
                    destination,
                    options,
                    json_cols=nested,
                )
        else:
            self.render_templates(relation, columns, destination, options)

    def get_templates_context(self, relation, columns, json_cols=None):
        metadata_cols = self.get_metadata_columns(relation, columns)
        context = {
            "relation": relation,
            "columns": metadata_cols,
            "nested": {},
            "adapter_name": self.adapter.__class__.__name__,
        }
        if json_cols:
            context["nested"] = self.get_nested_keys(json_cols, relation)
            # Removing original column with JSON data
            new_cols = []
            for col in metadata_cols:
                if col["id"].lower() not in context["nested"]:
                    new_cols.append(col)
            context["columns"] = new_cols
        config_db = self.get_config_value("database")
        if config_db:
            context["source_database"] = config_db

        return context

    def render_templates_with_context(self, context, destination, options):
        templates_folder = self.get_config_value("templates_folder")

        # Render model SQL
        render_template_file(
            "source_model.sql", context, destination, templates_folder=templates_folder
        )

        # Render model and source YMLs
        self.render_properties(context, options, templates_folder)

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

    def merge_models(self, models_a: list, models_b: list):
        models_a_names = [model.get("name") for model in models_a]
        for new_model in models_b:
            if not new_model.get("name") in models_a_names:
                models_a.append(new_model)
            else:
                for current_model in models_a:
                    if current_model.get("name") == new_model.get("name"):
                        self.update_model_properties(current_model, new_model)

        return models_a

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

    def update_sources_properties(self, source_a: dict, source_b: dict):
        source_a["database"] = source_b.get("database")
        source_a["schema"] = source_b.get("schema")
        self.update_source_tables(source_a.get("tables"), source_b.get("tables"))

    def merge_sources(self, sources_a, sources_b):
        sources_a_names = [source.get("name") for source in sources_a]
        for new_source in sources_b:
            if not new_source.get("name") in sources_a_names:
                sources_a.append(new_source)
            else:
                for current_source in sources_a:
                    if current_source.get("name") == new_source.get("name"):
                        self.update_sources_properties(current_source, new_source)
        return sources_a

    def merge_property_files(self, dict_a, dict_b):
        result_dict = copy.deepcopy(dict_a)
        if "sources" in result_dict:
            result_dict["sources"] = self.merge_sources(
                result_dict["sources"], dict_b["sources"]
            )
        if "models" in result_dict:
            result_dict["models"] = self.merge_models(
                result_dict["models"], dict_b["models"]
            )

        return result_dict

    def update_property_file(self, template, context, yml_path, templates_folder):
        # Grab and open current yml dict
        current_yml = dict()
        new_yml = dict()
        with open(yml_path, "r") as file:
            current_yml = yaml.safe_load(file)

        # Render new one
        render_template_file(
            template,
            context,
            yml_path,
            templates_folder=templates_folder,
        )

        # Open it
        with open(yml_path, "r") as file:
            new_yml = yaml.safe_load(file)

        # Deep merge with old content
        merged_ymls = self.merge_property_files(current_yml, new_yml)

        with open(yml_path, "w") as file:
            yaml.safe_dump(merged_ymls, file, sort_keys=False)

    def render_property_file(self, template, context, model_yml, templates_folder):
        model_yml.parent.mkdir(parents=True, exist_ok=True)
        render_template_file(
            template,
            context,
            model_yml,
            templates_folder=templates_folder,
        )

    def render_property_files(
        self, context, options, templates_folder, update_strategy, object
    ):
        strategy_key_update_all = ""
        strategy_key_recreate_all = ""

        if object == "Models":
            template = "source_model_props.yml"
            yml_cfg_destination = self.get_config_value("model_props_destination")
            # If destination does not contain Jinja syntax it's a single file,
            # and update/recreate must behave as 'update all' or 'recreate all'
            if not re.search(
                r"\{\{[A-Za-z]*\}\}", yml_cfg_destination.replace(" ", "")
            ):
                options["model_prop_is_single_file"] = True
            strategy_key_update_all = "model_prop_update_all"
            strategy_key_recreate_all = "model_prop_recreate_all"
        if object == "Sources":
            template = "source_props.yml"
            yml_cfg_destination = self.get_config_value("sources_destination")
            strategy_key_update_all = "source_prop_update_all"
            strategy_key_recreate_all = "source_prop_recreate_all"
            if not re.search(
                r"\{\{[A-Za-z]*\}\}", yml_cfg_destination.replace(" ", "")
            ):
                options["source_prop_is_single_file"] = True

        rel = context["relation"]
        yml_dest = self.generate_template(yml_cfg_destination, rel)
        yml_path = Path().joinpath(yml_dest)

        context["model"] = rel.name.lower()

        if (
            not options[strategy_key_recreate_all]
            and not options[strategy_key_update_all]
        ):
            if yml_path.exists():
                if update_strategy == "ask":
                    overwrite = questionary.select(
                        f"Property file {yml_path} already exists. What would you like to do with it?",
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
                    if overwrite == "Recreate":
                        self.render_property_file(
                            template, context, yml_path, templates_folder
                        )
                    if overwrite == "Recreate all":
                        options[strategy_key_recreate_all] = True
                        self.render_property_file(
                            template, context, yml_path, templates_folder
                        )
                    if overwrite == "Update":
                        if options.get("source_prop_is_single_file") or options.get(
                            "model_prop_is_single_file"
                        ):
                            options[strategy_key_update_all] = True
                        self.update_property_file(
                            template, context, yml_path, templates_folder
                        )

                    if overwrite == "Update all":
                        options[strategy_key_update_all] = True
                        self.update_property_file(
                            template, context, yml_path, templates_folder
                        )
                    if overwrite == "Skip":
                        pass
                    if overwrite == "Cancel":
                        exit
                elif update_strategy == "update":
                    self.update_property_file(
                        template, context, yml_path, templates_folder
                    )
                elif update_strategy == "recreate":
                    self.render_property_file(
                        template, context, yml_path, templates_folder
                    )
                else:
                    exit
            else:
                self.render_property_file(template, context, yml_path, templates_folder)
        if options[strategy_key_recreate_all]:
            self.render_property_file(template, context, yml_path, templates_folder)
        if options[strategy_key_update_all]:
            self.update_property_file(template, context, yml_path, templates_folder)

    def render_properties(self, context, options, templates_folder):
        update_strategy = self.get_config_value("update_strategy").lower()

        self.render_property_files(
            context, options, templates_folder, update_strategy, "Models"
        )
        self.render_property_files(
            context, options, templates_folder, update_strategy, "Sources"
        )

    def get_nested_keys(self, json_cols, relation):
        config_db = self.get_config_value("database")
        if config_db:
            config_db += "."
        else:
            config_db = ""
        _, data = self.adapter.execute(
            f"SELECT {', '.join(json_cols)} FROM {config_db}{relation.schema}.{relation.name} limit 1",
            fetch=True,
        )
        result = dict()
        if len(data.rows) > 0:
            for idx, json_col in enumerate(json_cols):
                value = data.columns[idx]
                try:
                    nested_key_names = list(json.loads(value[0]).keys())
                    result[json_col] = {}
                    for key_name in nested_key_names:
                        result[json_col][key_name] = self.get_default_metadata_item(
                            key_name
                        )
                    self.add_metadata_to_nested(relation, result, json_col)
                except TypeError:
                    console.print(
                        f"Column {json_col} in relation {relation.name} contains invalid JSON.\n"
                    )
        return result

    def add_metadata_to_nested(self, relation, data, col):
        """
        Adds metadata info to each nested field if metadata was provided.
        """
        metadata = self.get_metadata()
        if metadata:
            # Iterate over fields
            for item in data[col].keys():
                metadata_map_key_data = {
                    "database": relation.database,
                    "schema": relation.schema,
                    "relation": relation.name,
                    "column": col,
                    "key": item,
                }
                metadata_key = self.get_metadata_map_key(metadata_map_key_data)
                # Get metadata info or default and assign to the field.
                metadata_info = metadata.get(
                    metadata_key, self.get_default_metadata_item(item)
                )
                data[col][item] = metadata_info

    def run(self):
        config_database = self.get_config_value("database")
        self.db = config_database or self.config.credentials.database

        # initiate connection
        with self.adapter.connection_named("master"):

            filtered_schemas = self.get_schemas()
            if not filtered_schemas:
                return 0

            relations = self.get_relations(filtered_schemas)
            if relations:
                selected_relations = self.select_relations(relations)
                if selected_relations:
                    self.generate(selected_relations)
                else:
                    return 0
            else:
                schema_nlg = f"schema{'s' if len(filtered_schemas) > 1 else ''}"
                console.print(
                    f"No tables/views found in [u]{', '.join(filtered_schemas)}[/u] {schema_nlg}."
                )
        return 0
