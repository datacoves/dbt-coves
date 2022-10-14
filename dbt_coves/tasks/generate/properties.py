import glob
import json
import os
import subprocess
from pathlib import Path

import questionary
from questionary import Choice
from rich.console import Console

from .base import BaseGenerateTask

console = Console()


class GeneratePropertiesException(Exception):
    pass


class GeneratePropertiesTask(BaseGenerateTask):
    """
    Task that generate sources, models and model properties automatically
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "properties",
            parents=[base_subparser],
            help="Generate dbt models property files by inspecting the database schemas and relations.",
        )
        subparser.add_argument(
            "--templates-folder",
            type=str,
            help="Folder with jinja templates that override default "
            "sources generation templates, i.e. 'templates'",
        )
        subparser.add_argument(
            "--model-props-strategy",
            type=str,
            help="Strategy for model properties file generation," " i.e. 'one_file_per_model'",
        )
        subparser.add_argument(
            "--metadata",
            type=str,
            help="Path to csv file containing metadata, i.e. 'metadata.csv'",
        )
        subparser.add_argument(
            "--destination",  # TODO: should we copy the naming 'model-props-destination' from generate-sources?
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
            "--models",
            type=str,
            help="Path to look for SQL files and generate their property files, i.e.: 'models/staging/my_model.sql'",
        )
        cls.arg_parser = base_subparser

        subparser.set_defaults(cls=cls, which="properties")
        return subparser

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.json_from_dbt_ls = None

    def list_from_dbt_ls(self, output):

        if self.json_from_dbt_ls:
            return self.json_from_dbt_ls

        model_paths = self.get_config_value("models")
        if model_paths:
            dbt_params = ["dbt", "ls", "--output", output, "--model", model_paths]
        else:
            # Runtime Error
            # "models" and "resource_type" are mutually exclusive arguments
            dbt_params = ["dbt", "ls", "--output", output, "--resource-type", "model"]

        result = subprocess.run(dbt_params, capture_output=True, text=True)

        if result.returncode != 0:
            raise GeneratePropertiesException(
                f"An error occurred listing your dbt models: \n {result.stdout or result.stderr}"
            )

        manifest_json_lines = filter(
            lambda i: len(i) > 0 and i[0] == "{", result.stdout.splitlines()
        )

        manifest_data = [json.loads(model) for model in manifest_json_lines]
        manifest_data = filter(lambda d: d["resource_type"] == "model", manifest_data)

        return list(manifest_data)

    def load_manifest_nodes(self):

        path_pattern = f"{os.getcwd()}/**/manifest.json"
        manifest_path = glob.glob(path_pattern)[0]

        if not manifest_path:
            raise GeneratePropertiesException(f"Could not find manifest.json")

        with open(manifest_path, "r") as manifest:
            manifest_data = manifest.read()

        data = json.loads(manifest_data)

        return data

    def get_config_value(self, key):
        return self.coves_config.integrated["generate"]["properties"].get(key)

    def select_properties(self, models):
        selected_properties = questionary.checkbox(
            "Which properties would you like to generate?",
            choices=[Choice(model, checked=True, value=model) for model in models],
        ).ask()

        return selected_properties

    def select_models(self, manifest, dbt_models):
        dbt_models_manifest_naming = [
            f"{model['resource_type']}.{model['package_name']}.{model['name']}"
            for model in dbt_models
        ]

        # Filter user selection
        if len(dbt_models_manifest_naming) == 1:
            return dbt_models_manifest_naming
        else:
            return self.select_properties(dbt_models_manifest_naming)

    def generate(self, models, manifest):
        prop_destination = self.get_config_value("destination")
        options = {
            "model_prop_is_single_file": False,
            "model_prop_update_all": False,
            "model_prop_recreate_all": False,
        }
        for model in models:
            model_data = manifest["nodes"][model]
            database, schema, table = (
                model_data["database"],
                model_data["schema"],
                model_data["name"],
            )
            relation = self.adapter.get_relation(database, schema, table)
            if relation:
                columns = self.adapter.get_columns_in_relation(relation)
                model_destination = self.generate_template(prop_destination, relation)
                model_path = Path().joinpath(model_destination)

                self.render_templates(relation, columns, model_path, options)

            else:
                console.print(
                    f"Model [red]{schema}.{table}[/red] not materialized, did you execute [u][i]dbt run[/i][/u]?. "
                )
                continue

    def generate_properties(self, relation, columns, destination):
        destination.parent.mkdir(parents=True, exist_ok=True)
        self.render_templates(relation, columns, destination)

    def get_templates_context(self, relation, columns, json_cols=None):
        metadata_cols = self.get_metadata_columns(relation, columns)
        return {
            "relation": relation,
            "columns": metadata_cols,
            "adapter_name": self.adapter.__class__.__name__,
            "model": relation.name.lower(),
        }

    def render_templates_with_context(self, context, destination, options):
        templates_folder = self.get_config_value("templates_folder")
        update_strategy = self.get_config_value("update_strategy").lower()
        path = self.get_config_value("destination")
        self.render_property_files(
            context,
            options,
            templates_folder,
            update_strategy,
            "models",
            path,
            "model_props.yml",
        )

    def run(self):
        with self.adapter.connection_named("master"):
            dbt_models = self.list_from_dbt_ls("json")
            manifest = self.load_manifest_nodes()
            models = self.select_models(manifest, dbt_models)
            if models:
                self.generate(models, manifest)
            else:
                console.print(f"No models found with missing properties file.")

            return 0
