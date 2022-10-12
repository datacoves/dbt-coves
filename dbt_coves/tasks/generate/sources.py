from __future__ import nested_scopes

import json
import re
from pathlib import Path

import questionary
from questionary import Choice
from rich.console import Console

from dbt_coves.utils.jinja import render_template_file

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
            help="Where sources yml files will be generated, default: "
            "'models/staging/{{schema}}/sources.yml'",
        )
        subparser.add_argument(
            "--models-destination",
            type=str,
            help="Where models sql files will be generated, default: "
            "'models/staging/{{schema}}/{{relation}}.sql'",
        )
        subparser.add_argument(
            "--model-props-destination",
            type=str,
            help="Where models yml files will be generated, default: "
            "'models/staging/{{schema}}/{{relation}}.yml'",
        )
        subparser.add_argument(
            "--update-strategy",
            type=str,
            help="Action to perform when a property file already exists: "
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
        rel_name_selectors = [relation.upper() for relation in self.get_config_value("relations")]
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
            relation for relation in listed_relations if relation.name in rel_name_selectors
        ]
        rels = (
            intersected_rels if rel_name_selectors and rel_name_selectors[0] else listed_relations
        )

        return rels

    def select_relations(self, rels):
        selected_rels = questionary.checkbox(
            "Which sources would you like to generate?",
            choices=[
                Choice(f"[{rel.schema}] {rel.name}", checked=True, value=rel) for rel in rels
            ],
        ).ask()

        return selected_rels

    def generate(self, rels):
        models_destination = self.get_config_value("models_destination")
        options = {
            "override_all": None,
            "flatten_all": None,
            "model_prop_update_all": False,
            "model_prop_recreate_all": False,
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
        update_strategy = self.get_config_value("update_strategy").lower()

        # Render model SQL
        render_template_file(
            "source_model.sql", context, destination, templates_folder=templates_folder
        )

        # Render model and source YMLs
        model_property_destination = self.get_config_value("model_props_destination")
        self.render_property_files(
            context,
            options,
            templates_folder,
            update_strategy,
            "models",
            model_property_destination,
            "source_model_props.yml",
        )

        source_property_destination = self.get_config_value("sources_destination")
        self.render_property_files(
            context,
            options,
            templates_folder,
            update_strategy,
            "sources",
            source_property_destination,
            "source_props.yml",
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
                        result[json_col][key_name] = self.get_default_metadata_item(key_name)
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
                metadata_info = metadata.get(metadata_key)
                if metadata_info:
                    data[col][item].update(metadata_info)

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
