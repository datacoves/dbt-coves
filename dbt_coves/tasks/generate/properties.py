import glob
import json
import os
import subprocess
import questionary
from questionary import Choice
from rich.console import Console

from dbt_coves.utils.jinja import render_template_file

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
            "--select",
            type=str,
            help="dbt select. Specify the nodes to include.",
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
            help="Strategy for model properties file generation,"
            " i.e. 'one_file_per_model'",
        )
        subparser.add_argument(
            "--metadata",
            type=str,
            help="Path to csv file containing metadata, i.e. 'metadata.csv'",
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

        dbt_params = ["dbt", "ls", "--output", output, "--resource-type", "model"]
        select_argument = self.get_config_value("select")
        if select_argument:
            dbt_params += select_argument.split()

        result = subprocess.run(dbt_params, capture_output=True, text=True)

        if result.returncode != 0:
            raise GeneratePropertiesException(
                f"An error occurred listing your dbt models: \n {result.stdout or result.stderr}"
            )

        manifest_json_lines = filter(lambda i: len(i) > 0 and i[0] == "{", result.stdout.splitlines())

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

    def get_models_missing_profile(self, models, manifest):
        return [
            model
            for model in models
            if (
                model in manifest["nodes"].keys()
                and not manifest["nodes"][model]["patch_path"]
            )
        ]

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
        # Filter those that don't have patch_path
        dbt_models_missing_profile = self.get_models_missing_profile(
            dbt_models_manifest_naming, manifest
        )
        # Filter user selection
        if dbt_models_missing_profile:
            return self.select_properties(dbt_models_missing_profile)
        else:
            return list()

    # def get_column_data_for_relation(self, relation):
    #     columns = self.adapter.get_columns_in_relation(relation)
    #     column_data = list()
    #     for col in columns:
    #         column_dict = dict()
    #         column_dict["id"] = col.column
    #         column_dict["description"] = ""
    #         column_data.append(column_dict)
    #     return column_data

    def generate(self, models, manifest):
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
                destination = {
                    "name": model_data["name"],
                    "path": model_data["original_file_path"],
                }
                self.generate_properties(relation, columns, destination)
            else:
                print(f"No columns found on relation {schema}.{table}. Did you run 'dbt run' recently?")

    def generate_properties(self, relation, columns, destination):
        self.render_templates(relation, columns, destination)

    def render_templates_with_context(self, context, destination):
        context["model"] = destination["name"].lower()
        templates_folder = self.get_config_value("templates_folder")
        render_template_file(
            "model_props.yml",
            context,
            str(destination["path"]).replace(".sql", ".yml"),
            templates_folder=templates_folder,
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
