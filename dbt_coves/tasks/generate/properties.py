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

    def list_json_from_dbt_ls(self):

        if self.json_from_dbt_ls:
            return self.json_from_dbt_ls

        dbt_params = ["dbt", "ls", "--output", "json", "--resource-type", "model"]
        select_argument = self.get_config_value('select')
        if select_argument:
            dbt_params += ["--select", select_argument]

        result = subprocess.run(dbt_params, capture_output=True, text=True)
        manifest_json_lines = filter(lambda i: len(i) > 0, result.stdout.splitlines())
        manifest_data = [json.loads(model) for model in manifest_json_lines]
        manifest_data = filter(lambda d: d["resource_type"] == "model", manifest_data)
        # TODO: handle errors

        data = [x for x in manifest_data]

        self.json_from_dbt_ls = data

        return data

    def get_config_value(self, key):
        return self.coves_config.integrated["generate"]["properties"].get(key)

    def get_relations(self, filtered_schemas):

        target_relations = []
        for schema in filtered_schemas:
            relations = self.adapter.list_relations(self.db, schema)
            target_relations += relations

        filtered_relation_names = self.filter_relation_names()
        not_found = [x for x in filtered_relation_names if x.lower() not in [t.name.lower() for t in target_relations]]
        if not_found:
            relation_nlg = f"relation{'s' if len(not_found) > 1 else ''}"
            console.print(
                f"Provided {relation_nlg} [u]{', '.join(not_found)}[/u] not found in Database.\n"
            )

        target_relations = [x for x in target_relations if x.name.lower() in filtered_relation_names]

        return target_relations

    def filter_relation_names(self):

        manifest = self.list_json_from_dbt_ls()
        paths = [item["original_file_path"] for item in manifest]
        relation_names = [
            item.split("/")[-1].split(".sql")[0].lower()
            for item in paths
            if not os.path.exists(item.replace(".sql", ".yml"))
        ]

        return relation_names

    def generate(self, rels):
        manifest = self.list_json_from_dbt_ls()
        for rel in rels:
            destinations = [{"name": x["name"], "path": x["original_file_path"]} for x in manifest if x["name"].lower() == rel.name.lower()]
            if destinations:
                destination = destinations[0]
                self.generate_properties(rel, destination)

    def generate_properties(self, relation, destination):
        columns = self.adapter.get_columns_in_relation(relation)
        self.render_templates(relation, columns, destination)

    def render_templates_render_context(self, context, destination):
        context["model"] = destination["name"].lower()
        templates_folder = self.get_config_value("templates_folder")
        render_template_file(
            "model.yml",
            context,
            str(destination["path"]).replace(".sql", ".yml"),
            templates_folder=templates_folder,
        )
