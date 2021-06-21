import questionary
from pathlib import Path
from questionary import Choice
from rich.console import Console
from dbt.adapters.factory import get_adapter

from dbt_coves.tasks.base import BaseTask


console = Console()


class GenerateSourcesTask(BaseTask):
    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "sources", parents=[base_subparser], help="Generate source dbt models reading database schema."
        )
        subparser.add_argument(
            "--schemas",
            type=str,
            help="""
            Comma separated list of schemas where raw data resides, i.e. 'RAW_SALESFORCE,RAW_HUBSPOT'
            """
        )
        subparser.add_argument(
            "--destination",
            type=str,
            help="""
            Where models sql files will be generated, i.e. 'models/{schema_name}/{relation_name}.sql'
            """
        )
        subparser.add_argument(
            "--model_props_strategy",
            type=str,
            help="""
            Strategy for model properties files generation, i.e. 'one_file_per_model'
            """
        )
        subparser.set_defaults(cls=cls)
        return subparser

    def run(self):
        adapter = get_adapter(self.config)
        db = self.config.credentials.database
        schema_names = [schema.strip().upper() for schema in self.get_config_value('schemas').split(',')]

        with adapter.connection_named('master'):
            schemas = [schema.upper() for schema in adapter.list_schemas(db) if schema != "INFORMATION_SCHEMA"]
            filtered_schemas = list(set(schemas).intersection(schema_names))
            if not filtered_schemas:
                schema_nlg = f"schema{'s' if len(schema_names) > 1 else ''}"
                console.print(f"Provided {schema_nlg} [u]{', '.join(schema_names)}[/u] not found in Database.\n")
                selected_schemas = questionary.checkbox(
                    "Which schemas would you like to inspect?",
                    choices=[Choice(schema, checked=True) if "RAW" in schema else Choice(schema) for schema in schemas]).ask()
                if selected_schemas:
                    filtered_schemas = selected_schemas
                else:
                    return 0
            rels = []
            for schema in filtered_schemas:
                rels += adapter.list_relations(db, schema)

            if rels:
                selected_rels = questionary.checkbox(
                    "Which sources would you like to generate?",
                    choices=[Choice(f"[{rel.schema}] {rel.name}", checked=True) for rel in rels]).ask()
                if selected_rels:
                    self.generate_sources(rels)
                else:
                    return 0
            else:
                schema_nlg = f"schema{'s' if len(filtered_schemas) > 1 else ''}"
                console.print(f"No tables/views found in [u]{', '.join(filtered_schemas)}[/u] {schema_nlg}.")
                # cols = adapter.get_columns_in_relation(rels[0])
                # print(cols)
        return 0

    def get_config_value(self, key):
        return self.coves_config.integrated['generate']['sources'][key]

    def generate_sources(self, rels):
        dest = self.get_config_value('destination')
        override_all = None
        flatten_all = None
        for rel in rels:
            model_dest = dest.format(schema_name=rel.schema.lower(), relation_name=rel.name.lower())
            model_sql = Path().joinpath(model_dest)
            if not override_all:
                if model_sql.exists():
                    overwrite = questionary.select(f"{model_dest} already exists. Would you like to overwrite it?",
                        choices=[
                            "No",
                            "Yes",
                            "No for all",
                            "Yes for all"
                        ], default="No").ask()
                    if overwrite == "Yes":
                        self.generate_model(rel, model_sql, flatten_all)
                    elif overwrite == "No for all":
                        override_all = "No"
                    elif overwrite == "Yes for all":
                        override_all = "Yes"
                else:
                    self.generate_model(rel, model_sql, flatten_all)
            elif override_all == "yes":
                self.generate_model(rel, model_sql, flatten_all)
            else:
                if not model_sql.exists():
                    self.generate_model(rel, model_sql, flatten_all)

    def generate_model(self, rel, destination, flatten_all):
        destination.parent.mkdir(parents=True, exist_ok=True)
        with open(destination, "w") as sql_file:
            sql_file.write("Test")