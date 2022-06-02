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
            help="Generate source dbt models by inspecting the database schemas and relations.",
        )
        subparser.add_argument(
            "--select",
            type=str,
            help="dbt select. Specify the nodes to include.",
        )
        subparser.add_argument(
            "--templates_folder",
            type=str,
            help="Folder with jinja templates that override default "
            "sources generation templates, i.e. 'templates'",
        )

        subparser.set_defaults(cls=cls, which="properties")
        return subparser

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.json_from_dbt_ls = None

    def list_from_dbt_ls(self, output):

        if self.json_from_dbt_ls:
            return self.json_from_dbt_ls

        dbt_params = ["dbt", "ls", "--output", output, "--resource-type", "model"]
        select_argument = self.get_config_value("dbt-selector")
        if select_argument:
            dbt_params += select_argument.split()

        print(dbt_params)
        result = subprocess.run(dbt_params, capture_output=True, text=True)
        manifest_json_lines = filter(lambda i: len(i) > 0, result.stdout.splitlines())

        manifest_data = [json.loads(model) for model in manifest_json_lines]
        manifest_data = filter(lambda d: d["resource_type"] == "model", manifest_data)

        return list(manifest_data)

    def load_manifest_nodes(self):

        path_pattern = f"{os.getcwd()}/**/manifest.json"
        manifest_path = glob.glob(path_pattern)[0]
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

    def get_models(self, manifest):
        dbt_models = self.list_from_dbt_ls("json")
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
            raise GeneratePropertiesException(
                f"No models with missing property file were found"
            )

    def generate(self, models, manifest):
        import ipdb  # TODO ipdb import

        for model in models:
            ipdb.set_trace()  # TODO remove trace()
            model_data = manifest["nodes"][model]
            # TODO better key formatting
            column_data = model_data["columns"]
            if not column_data:
                continue
            destination = {
                "name": model_data["name"],
                "path": model_data["original_file_path"],
            }
            self.generate_properties(model, column_data, destination)

    def generate_properties(self, model, column_data, destination):
        self.render_templates(model, column_data, destination)

    def render_templates(self, model, column_data, destination):

        context = {}  # TODO fill context
        self.render_templates_render_context(context, destination)

    def render_templates_render_context(self, context, destination):
        context["model"] = destination["name"].lower()
        templates_folder = self.get_config_value("templates_folder")
        render_template_file(
            "model.yml",
            context,
            str(destination["path"]).replace(".sql", ".yml"),
            templates_folder=templates_folder,
        )

    def run(self):
        manifest = self.load_manifest_nodes()
        import ipdb  # TODO ipdb import

        ipdb.set_trace()  # TODO remove trace()
        models = self.get_models(manifest)
        print("*******")
        print(models)
        if models:
            self.generate(models, manifest)
        else:
            console.print(f"No models found with missing properties file")

        return 0
