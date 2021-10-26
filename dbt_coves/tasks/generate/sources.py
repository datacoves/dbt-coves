import json
from pathlib import Path
import re

import questionary
from questionary import Choice
from rich.console import Console

from dbt_coves.tasks.base import BaseConfiguredTask
from dbt_coves.utils.jinja import render_template, render_template_file

console = Console()


NESTED_FIELD_TYPES = {
    "SnowflakeAdapter": "VARIANT",
    "BigQueryAdapter": "STRUCT",
    "RedshiftAdapter": "SUPER",
}


class GenerateSourcesTask(BaseConfiguredTask):
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
            "--destination",
            type=str,
            help="Where models sql files will be generated, i.e. "
            "'models/{schema_name}/{relation_name}.sql'",
        )
        subparser.add_argument(
            "--model_props_strategy",
            type=str,
            help="Strategy for model properties files generation,"
            " i.e. 'one_file_per_model'",
        )
        subparser.add_argument(
            "--templates_folder",
            type=str,
            help="Folder with jinja templates that override default "
            "sources generation templates, i.e. 'templates'",
        )
        subparser.set_defaults(cls=cls, which="sources")
        return subparser

    def run(self):
        config_database = self.get_config_value("database")
        db = config_database or self.config.credentials.database
        schema_name_selectors = [
            schema.upper() for schema in self.get_config_value("schemas")
        ]

        schema_wildcard_selectors = []
        for schema_name in schema_name_selectors:
            if "*" in schema_name:
                schema_wildcard_selectors.append(schema_name.replace("*", ".*"))
        with self.adapter.connection_named("master"):
            schemas = [
                schema.upper()
                for schema in self.adapter.list_schemas(db)
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
                selected_schemas = questionary.checkbox(
                    "Which schemas would you like to inspect?",
                    choices=[
                        Choice(schema, checked=True)
                        if "RAW" in schema
                        else Choice(schema)
                        for schema in schemas
                    ],
                ).ask()
                if selected_schemas:
                    filtered_schemas = selected_schemas
                else:
                    return 0

            rel_name_selectors = [
                relation.upper() for relation in self.get_config_value("relations")
            ]
            rel_wildcard_selectors = []
            for rel_name in rel_name_selectors:
                if "*" in rel_name:
                    rel_wildcard_selectors.append(rel_name.replace("*", ".*"))

            listed_relations = []
            for schema in filtered_schemas:
                listed_relations += self.adapter.list_relations(db, schema)

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

            if rels:
                selected_rels = questionary.checkbox(
                    "Which sources would you like to generate?",
                    choices=[
                        Choice(f"[{rel.schema}] {rel.name}", checked=True, value=rel)
                        for rel in rels
                    ],
                ).ask()
                if selected_rels:
                    self.generate_sources(selected_rels)
                else:
                    return 0
            else:
                schema_nlg = f"schema{'s' if len(filtered_schemas) > 1 else ''}"
                console.print(
                    f"No tables/views found in [u]{', '.join(filtered_schemas)}[/u] {schema_nlg}."
                )
        return 0

    def get_config_value(self, key):
        return self.coves_config.integrated["generate"]["sources"][key]

    def generate_sources(self, rels):
        dest = self.get_config_value("destination")
        options = {"override_all": None, "flatten_all": None}
        for rel in rels:
            model_dest = render_template(
                dest, {"schema": rel.schema.lower(), "relation": rel.name.lower()}
            )
            model_sql = Path().joinpath(model_dest)
            if not options["override_all"]:
                if model_sql.exists():
                    overwrite = questionary.select(
                        f"{model_dest} already exists. Would you like to overwrite it?",
                        choices=["No", "Yes", "No for all", "Yes for all"],
                        default="No",
                    ).ask()
                    if overwrite == "Yes":
                        self.generate_model(rel, model_sql, options)
                    elif overwrite == "No for all":
                        options["override_all"] = "No"
                    elif overwrite == "Yes for all":
                        options["override_all"] = "Yes"
                        self.generate_model(rel, model_sql, options)
                else:
                    self.generate_model(rel, model_sql, options)
            elif options["override_all"] == "Yes":
                self.generate_model(rel, model_sql, options)
            else:
                if not model_sql.exists():
                    self.generate_model(rel, model_sql, options)

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
                    self.render_templates(relation, columns, destination, nested=nested)
                elif flatten == "No":
                    self.render_templates(relation, columns, destination)
                elif flatten == "No for all":
                    options["flatten_all"] = "No"
                    self.render_templates(relation, columns, destination)
                elif flatten == "Yes for all":
                    options["flatten_all"] = "Yes"
                    self.render_templates(relation, columns, destination, nested=nested)
            else:
                self.render_templates(relation, columns, destination)
        elif options["flatten_all"] == "Yes":
            if nested:
                self.render_templates(relation, columns, destination, nested=nested)
        else:
            self.render_templates(relation, columns, destination)

    def get_nested_keys(self, columns, schema, relation):
        config_db = self.get_config_value("database")
        if config_db:
            config_db += "."
        else:
            config_db = ""
        _, data = self.adapter.execute(
            f"SELECT {', '.join(columns)} FROM {config_db}{schema}.{relation} limit 1",
            fetch=True,
        )
        result = dict()
        if len(data.rows) > 0:
            for idx, col in enumerate(columns):
                value = data.columns[idx]
                try:
                    result[col] = list(json.loads(value[0]).keys())
                except TypeError:
                    console.print(
                        f"Column {col} in relation {relation} contains invalid JSON.\n"
                    )
        return result

    def render_templates(self, relation, columns, destination, nested=None):
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

        templates_folder = self.get_config_value("templates_folder")
        render_template_file(
            "source_model.sql", context, destination, templates_folder=templates_folder
        )
        context["model"] = destination.name.lower().replace(".sql", "")
        render_template_file(
            "source_model_props.yml",
            context,
            str(destination).replace(".sql", ".yml"),
            templates_folder=templates_folder,
        )
