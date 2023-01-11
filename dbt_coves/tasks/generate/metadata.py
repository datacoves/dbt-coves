import csv
import json
from pathlib import Path

import questionary
from questionary import Choice
from rich.console import Console

from dbt_coves.utils.jinja import render_template

from .base import BaseGenerateTask

console = Console()


class GenerateMetadataTask(BaseGenerateTask):
    """
    Task that generates a metadata file based on a relation
    """

    METADATA_HEADERS = [
        "database",
        "schema",
        "relation",
        "column",
        "key",
        "type",
        "description",
    ]

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "metadata",
            parents=[base_subparser],
            help="Generate metadata file by inspecting database schemas and relations.",
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
            "--select-relations",
            type=str,
            help="Comma separated list of relations where raw data resides, "
            "i.e. 'RAW_HUBSPOT_PRODUCTS,RAW_SALESFORCE_USERS'",
        )
        subparser.add_argument(
            "--exclude-relations",
            type=str,
            help="Filter relation(s) to exclude from source file(s) generation",
        )
        subparser.add_argument(
            "--destination", type=str, help="Generated metadata destination path"
        )

        cls.arg_parser = base_subparser
        subparser.set_defaults(cls=cls, which="metadata")
        return subparser

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = None
        self.metadata_files_processed = set()

    def get_config_value(self, key):
        return self.coves_config.integrated["generate"]["metadata"][key]

    def select_relations(self, rels):
        selected_rels = questionary.checkbox(
            "Which metadata files would you like to generate?",
            choices=[Choice(f"[{rel.schema}] {rel.name}", checked=True, value=rel) for rel in rels],
        ).ask()

        return selected_rels

    def render_path_template(self, destination_path, relation):
        template_context = {
            "schema": relation.schema.lower(),
            "relation": relation.name.lower(),
        }
        return render_template(destination_path, template_context)

    def get_nested_columns(self, columns):
        nested_field_type = self.NESTED_FIELD_TYPES.get(self.adapter.__class__.__name__)
        return [col.name.lower() for col in columns if col.dtype == nested_field_type]

    def get_shared_column_fields(self, relation):
        return {
            "database": relation.database.lower(),
            "schema": relation.schema.lower(),
            "relation": relation.name.lower(),
        }

    def get_csv_dicts(self, context):
        results = list()
        relation = context["relation"]
        for nested_col_name, nested_field in context.get("nested", {}).items():
            for name, data in nested_field.items():
                nested_dict = self.get_shared_column_fields(relation)
                nested_dict["column"] = nested_col_name.lower()
                nested_dict["key"] = data.get("name").lower()
                nested_dict["type"] = data.get("type")
                nested_dict["description"] = data.get("description", "")
                results.append(nested_dict)

        for column in context.get("columns"):
            column_dict = self.get_shared_column_fields(relation)
            column_dict["column"] = column.column.lower()
            column_dict["key"] = ""
            column_dict["type"] = column.dtype.lower()
            column_dict["description"] = ""
            results.append(column_dict)

        return results

    def generate_or_append_metadata(self, relation, destination, options, action, existing_rows):
        destination.parent.mkdir(parents=True, exist_ok=True)
        if action == "append":
            python_fs_action = "a"
        if action == "create":
            python_fs_action = "w"

        columns = self.adapter.get_columns_in_relation(relation)
        nested = self.get_nested_columns(columns)

        context = self.get_templates_context(relation, columns, nested)
        dicts_to_write = self.get_csv_dicts(context)

        with open(destination, python_fs_action, newline="") as csvfile:
            writer = csv.DictWriter(
                csvfile, fieldnames=self.METADATA_HEADERS, quoting=csv.QUOTE_ALL
            )
            if python_fs_action == "w":
                writer.writeheader()
            for csvdict in dicts_to_write:
                if existing_rows and python_fs_action == "a":
                    if csvdict not in existing_rows:
                        writer.writerow(csvdict)
                else:
                    writer.writerow(csvdict)
        console.print(
            f"[green]{relation.name}[/green] metadata written to "
            f"[green]{destination.absolute()}[/green]"
        )

    def get_nested_keys(self, json_cols, relation):
        config_db = self.get_config_value("database")
        if config_db:
            config_db += "."
        else:
            config_db = ""
        _, data = self.adapter.execute(
            f"SELECT {', '.join(json_cols)} FROM {config_db}{relation.schema}.{relation.name} \
                limit 1",
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
                except TypeError:
                    console.print(
                        f"Column {json_col} in relation {relation.name} contains invalid JSON.\n"
                    )
        return result

    def get_templates_context(self, relation, columns, nested):
        context = {
            "relation": relation,
            "columns": columns,
        }
        if nested:
            context["nested"] = self.get_nested_keys(nested, relation)
        return context

    def get_existing_csv_rows(self, destination):
        try:
            with open(destination, newline="") as csvfile:
                reader = csv.DictReader(csvfile)
                headers = reader.fieldnames
                if set(headers) == set(self.METADATA_HEADERS):
                    return [row for row in reader]
                else:
                    return []
        except FileNotFoundError:
            return []

    def generate(self, rels):
        destination = self.get_config_value("destination")
        options = {"append_all": False, "recreate_files": False}

        for rel in rels:
            csv_dest = self.render_path_template(destination, rel)
            csv_path = Path(self.config.project_root).joinpath(csv_dest)
            existing_rows = self.get_existing_csv_rows(csv_path)
            if csv_path.exists() and existing_rows:
                if (
                    csv_path not in self.metadata_files_processed
                    and not options["append_all"]
                    and not options["recreate_files"]
                ):
                    console.print(f"[yellow]{csv_path.absolute()}[/yellow] exists.")
                    append = questionary.select(
                        f"How would you like to append {rel.name.lower()} columns' metadata?",
                        choices=[
                            "Recreate file",
                            "Add missing columns",
                            "Cancel",
                        ],
                    ).ask()
                    if append == "Add missing columns":
                        action = "append"
                        options["append_all"] = True
                    elif append == "Recreate file":
                        action = "create"
                        options["recreate_files"] = True
                    elif action == "Cancel":
                        exit()
                elif csv_path in self.metadata_files_processed or options["append_all"]:
                    action = "append"
                elif options["recreate_files"]:
                    action = "create"
            else:
                action = "create"

            self.generate_or_append_metadata(rel, csv_path, options, action, existing_rows)
            self.metadata_files_processed.add(csv_path)

    def run(self):
        config_database = self.get_config_value("database")
        self.db = config_database or self.config.credentials.database

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
                    console.print("No relations selected for metadata generation")
                    return 0
            else:
                schema_nlg = f"schema{'s' if len(filtered_schemas) > 1 else ''}"
                console.print(
                    f"No tables/views found in [u]{', '.join(filtered_schemas)}[/u] {schema_nlg}."
                )
        return 0
