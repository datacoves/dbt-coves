from pathlib import Path

import questionary
from rich.console import Console

from dbt_coves.utils.api_caller import FivetranApiCaller
from dbt_coves.utils.yaml import open_yaml

from .base import BaseExtractTask

console = Console()


class FivetranExtractorException(Exception):
    pass


class ExtractFivetranTask(BaseExtractTask):
    """
    Task that extracts Fivetran destinations and connectors, and stores them as json files
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "fivetran",
            parents=[base_subparser],
            help="Extracts Fivetran destinations and connectors, and stores them as json files",
        )
        subparser.add_argument(
            "--path",
            type=str,
            help="""Path where configuration json files will be created,
            i.e. '/var/data/fivetran_extract/'""",
        )
        subparser.add_argument(
            "--api-key",
            type=str,
            help="Fivetran's API Key's secret",
        )
        subparser.add_argument(
            "--api-secret",
            type=str,
            help="Fivetran's API Secret's secret",
        )
        subparser.add_argument(
            "--credentials", type=str, help="Path to Fivetran credentials YAML file"
        )
        subparser.set_defaults(cls=cls, which="fivetran")
        return subparser

    def get_config_value(self, key):
        return self.coves_config.integrated["extract"]["fivetran"][key]

    def run(self) -> int:
        self.extraction_results = set()

        extract_destination = self.get_config_value("path")
        self.api_key = self.get_config_value("api_key")
        self.api_secret = self.get_config_value("api_secret")
        api_credentials_path = self.get_config_value("credentials")

        if api_credentials_path and (self.api_key or self.api_secret):
            raise FivetranExtractorException(
                "Flags 'credentials' and 'api key/secret' ones are mutually exclusive."
            )
        if not extract_destination or not (
            (self.api_key and self.api_secret) or api_credentials_path
        ):
            raise FivetranExtractorException(
                "Couldn't start extraction: one (or more) of the following arguments is missing: "
                "'path', 'api-key', 'api-secret', 'credentials'"
            )

        if api_credentials_path:
            self.fivetran_api = self._connect_to_api_using_credentials_file(
                Path(api_credentials_path)
            )
        else:
            self.fivetran_api = FivetranApiCaller(self.api_key, self.api_secret)
        self.extract_destination = Path(extract_destination)
        self.extract_destination.mkdir(exist_ok=True, parents=True)

        for (
            destination_name,
            destination_data,
        ) in self.fivetran_api.fivetran_data.items():
            group_id = destination_data["details"]["group_id"]
            group_name = self.fivetran_api.get_group_name(group_id)
            filename = f"{group_name.lower()}.json"
            destination_filepath = self.extract_destination.joinpath(filename)

            export_data = {destination_name: destination_data}

            self.save_json(destination_filepath, export_data)
            self.extraction_results.add(filename)
        if len(self.extraction_results) >= 1:
            console.print(
                f"Extraction to path {self.extract_destination} was successful\n"
                f"[u]Extracted[/u]: {self.extraction_results}\n"
            )
        else:
            console.print("No Fivetran Connections were extracted")
        return 0

    def _connect_to_api_using_credentials_file(self, credentials_path):
        api_key = None
        api_secret = None
        credentials = open_yaml(credentials_path)
        if len(credentials) > 1:
            fivetran_account = questionary.select(
                "Which of your Fivetran accounts will you use?:",
                choices=[account for account in credentials.keys()],
            ).ask()
            api_key = credentials[fivetran_account]["api_key"]
            api_secret = credentials[fivetran_account]["api_secret"]
        else:
            default_credentials = next(iter(credentials.values()))
            api_key = default_credentials["api_key"]
            api_secret = default_credentials["api_secret"]

        return FivetranApiCaller(api_key, api_secret)
