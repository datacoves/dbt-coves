import glob
import json
import os
import subprocess

from rich.console import Console

from dbt_coves.utils.jinja import render_template_file

from .base import BaseGenerateTask

console = Console()


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
        select_argument = self.get_config_value('select')
        if select_argument:
            dbt_params += ["--select", select_argument]

        print(dbt_params)
        result = subprocess.run(dbt_params, capture_output=True, text=True)
        manifest_json_lines = filter(lambda i: len(i) > 0, result.stdout.splitlines())

        manifest_data = [json.loads(model) for model in manifest_json_lines]
        manifest_data = filter(lambda d: d["resource_type"] == "model", manifest_data)

        return manifest_data

    def load_manifest_nodes(self):

        path_pattern = f"{os.getcwd()}/**/manifest.json"
        manifest_path = glob.glob(path_pattern)[0]
        with open(manifest_path, "r") as manifest:
            manifest_data = manifest.read()

        data = json.loads(manifest_data)

        return data

    def get_config_value(self, key):
        return self.coves_config.integrated["generate"]["properties"].get(key)


    def get_models(self):

        models = self.list_from_dbt_ls("json")

        return models

    def generate(self, models):
        manifest = self.load_manifest_nodes()
        for model in models:
            # TODO better key formatting
            column_data = manifest['nodes'][model['resource_type'] + "." + model["package_name"]  + "."  + model['name'] ]['columns']
            if not column_data:
                continue
            destination = {"name": model["name"], "path": model["original_file_path"]}
            self.generate_properties(model, column_data, destination)


    def generate_properties(self, model, column_data, destination):
        self.render_templates(model, column_data, destination)

    def render_templates(self, model, column_data, destination):

        context = {} # TODO fill context
        self.render_templates_render_context(context,  destination)

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

        models = self.get_models()
        print("*******")
        print(models)
        if models:
            self.generate(models)
        else:
            schema_nlg = f"schema{'s' if len(models) > 1 else ''}"
            console.print(
                f"No models found in [u]{', '.join(models)}[/u] {schema_nlg}."
            )

        return 0