import glob
import json
import os
import pathlib
import re
from copy import copy

from rich.console import Console

from dbt_coves.core.exceptions import MissingArgumentException
from dbt_coves.utils.api_caller import AirbyteApiCaller
from dbt_coves.utils.tracking import trackable

from .base import BaseExtractTask

console = Console()
NON_EXTRACT_KEYS = ["icon", "breakingChange", "createdAt", "updatedAt"]


class AirbyteExtractorException(Exception):
    pass


class ExtractAirbyteTask(BaseExtractTask):
    """
    Task that extracts airbyte sources, connections and destinations and stores them as json files
    """

    def _normalize_filename(self, name: str) -> str:
        """
        Normalize a string to be safe for filenames: lowercase, replace spaces and unsafe chars
        with underscores.
        """
        name = name.lower()
        name = re.sub(r"[^a-z0-9\-]+", "_", name)
        name = name.strip("_")
        return name

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "airbyte",
            parents=[base_subparser],
            help="""Extracts airbyte sources, connections and destinations
            and stores them as json files""",
        )
        subparser.add_argument(
            "--path",
            type=str,
            help="""Path where configuration json files will be created,
            i.e. '/var/data/airbyte_extract/'""",
        )
        subparser.add_argument(
            "--host",
            type=str,
            help="Airbyte's API host, i.e. 'http://airbyte-server:8000'",
        )
        subparser.add_argument(
            "--api-key",
            type=str,
            help="Airbyte's API key for Bearer token auth (optional for open OSS instances)",
        )
        subparser.set_defaults(cls=cls, which="airbyte")
        return subparser

    @trackable
    def run(self):
        self.extraction_results = {
            "sources": set(),
            "destinations": set(),
            "connections": set(),
        }

        extract_destination = self.get_config_value("path")
        airbyte_host = self.get_config_value("host")
        airbyte_api_key = self.get_config_value("api_key")

        if not extract_destination or not airbyte_host:
            raise MissingArgumentException(["path", "host"], self.coves_config)

        extract_destination = pathlib.Path(extract_destination)

        connections_path = extract_destination / "connections"
        sources_path = extract_destination / "sources"
        destinations_path = extract_destination / "destinations"

        self.connections_extract_destination = os.path.abspath(connections_path)
        self.destinations_extract_destination = os.path.abspath(destinations_path)
        self.sources_extract_destination = os.path.abspath(sources_path)

        self.airbyte_api = AirbyteApiCaller(airbyte_host, api_key=airbyte_api_key or None)

        console.print(
            "Extracting Airbyte's [b]Source[/b], [b]Destination[/b] and [b]Connection[/b]"
            f"configurations to {os.path.abspath(extract_destination)}\n"
        )

        sources_path.mkdir(exist_ok=True, parents=True)
        destinations_path.mkdir(exist_ok=True, parents=True)
        connections_path.mkdir(exist_ok=True, parents=True)

        for airbyte_source in self.airbyte_api.sources_list:
            source_id = airbyte_source["sourceId"]
            source_json = self._get_airbyte_source_from_id(source_id)
            self._save_json_source(source_json)

        for airbyte_destination in self.airbyte_api.destinations_list:
            destination_id = airbyte_destination["destinationId"]
            destination_json = self._get_airbyte_destination_from_id(destination_id)
            self._save_json_destination(destination_json)

        for airbyte_conn in self.airbyte_api.connections_list:
            self._save_json_connection(airbyte_conn)

        if len(self.extraction_results["sources"]) >= 1:
            console.print(
                f"Extraction to path {extract_destination} was successful!\n"
                f"[u]Sources[/u]: {self.extraction_results['sources']}\n"
                f"[u]Destinations[/u]: {self.extraction_results['destinations']}\n"
                f"[u]Connections[/u]: {self.extraction_results['connections']}\n"
            )
        else:
            console.print("No Airbyte Connections were extracted")
        return 0

    def dbt_packages_exist(self, dbt_project_path):
        return glob.glob(f"{str(dbt_project_path)}/dbt_packages")

    def _get_definition_id_by_connector_type(self, connector_type, definitions):
        """
        Find a connector definition ID by matching the connector type string
        (e.g. "postgres") against the docker repository name (e.g. "airbyte/source-postgres").
        """
        for definition in definitions:
            repo = definition.get("dockerRepository", "")
            repo_suffix = repo.split("/")[-1]  # e.g. "source-postgres"
            expected = f"source-{connector_type}"
            if repo_suffix == expected or repo_suffix == f"destination-{connector_type}":
                def_id = definition.get("sourceDefinitionId") or definition.get(
                    "destinationDefinitionId"
                )
                return def_id
            # Also try a direct connectorType field if the API provides one
            if definition.get("connectorType") == connector_type:
                return definition.get("sourceDefinitionId") or definition.get(
                    "destinationDefinitionId"
                )
        return None

    def _get_airbyte_destination_definition_spec(self, destination_type):
        definition_id = self._get_definition_id_by_connector_type(
            destination_type, self.airbyte_api.destination_definitions
        )
        if not definition_id:
            return None
        return self.airbyte_api.get_destination_spec(definition_id)

    def _get_airbyte_source_definition_spec(self, source_type):
        definition_id = self._get_definition_id_by_connector_type(
            source_type, self.airbyte_api.source_definitions
        )
        if not definition_id:
            return None
        return self.airbyte_api.get_source_spec(definition_id)

    def _get_airbyte_destination_from_id(self, destination_id):
        """Get the complete Destination object from its ID."""
        for destination in self.airbyte_api.destinations_list:
            if destination["destinationId"] == destination_id:
                destination_type = destination.get("destinationType", "")
                spec = self._get_airbyte_destination_definition_spec(destination_type)

                if spec:
                    airbyte_secret_fields = self._get_airbyte_secret_fields_for_definition(spec)
                    destination["configuration"] = self._hide_configuration_secret_fields(
                        destination.get("configuration", {}), airbyte_secret_fields
                    )
                else:
                    console.print(
                        f"[yellow]Warning:[/yellow] Could not retrieve spec for destination type "
                        f"'{destination_type}' — secrets will not be masked"
                    )

                destination["connectorVersion"] = self._get_connector_version(
                    destination_type, self.airbyte_api.destination_definitions, "destination"
                )
                return destination

        raise AirbyteExtractorException(
            "Airbyte extract error: there is no Airbyte"
            f"Destination for id [red]{destination_id}[/red]"
        )

    def _get_connector_version(self, connector_type, definitions, obj_type):
        """
        Look up dockerImageTag from the definitions list by matching connector type
        against the docker repository name.
        """
        for definition in definitions:
            repo = definition.get("dockerRepository", "")
            repo_suffix = repo.split("/")[-1]
            expected_suffix = f"{obj_type}-{connector_type}"
            if repo_suffix == expected_suffix:
                return definition.get("dockerImageTag", "unknown")
            if definition.get("connectorType") == connector_type:
                return definition.get("dockerImageTag", "unknown")
        console.print(
            f"[yellow]Warning:[/yellow] Could not find connector version for {obj_type} "
            f"type '{connector_type}'"
        )
        return "unknown"

    def _get_airbyte_source_from_id(self, source_id):
        """Get the complete Source object from its ID."""
        for source in self.airbyte_api.sources_list:
            if source["sourceId"] == source_id:
                source_type = source.get("sourceType", "")
                spec = self._get_airbyte_source_definition_spec(source_type)

                if spec:
                    airbyte_secret_fields = self._get_airbyte_secret_fields_for_definition(spec)
                    source["configuration"] = self._hide_configuration_secret_fields(
                        source.get("configuration", {}), airbyte_secret_fields
                    )
                else:
                    console.print(
                        f"[yellow]Warning:[/yellow] Could not retrieve spec for source type "
                        f"'{source_type}' — secrets will not be masked"
                    )

                source["connectorVersion"] = self._get_connector_version(
                    source_type, self.airbyte_api.source_definitions, "source"
                )
                return source

        raise AirbyteExtractorException(
            f"Airbyte extract error: there is no Airbyte Source for id [red]{source_id}[/red]"
        )

    def _hide_configuration_secret_fields(self, configuration, airbyte_secret_fields):
        for k, v in configuration.items():
            if isinstance(v, dict):
                self._hide_configuration_secret_fields(v, airbyte_secret_fields)
            elif k in airbyte_secret_fields:
                configuration[k] = "**********"
        return configuration

    def _get_airbyte_secret_fields_for_definition(
        self, definition, dict_name=None, secret_fields=[]
    ):
        try:
            for k, v in definition.items():
                if isinstance(v, dict):
                    self._get_airbyte_secret_fields_for_definition(v, k, secret_fields)
                else:
                    if "airbyte_secret" in str(k):
                        if bool(definition["airbyte_secret"]) and dict_name not in secret_fields:
                            secret_fields.append(dict_name)
            return secret_fields
        except KeyError as e:
            raise AirbyteExtractorException(
                f"There was an error searching secret fields for definition: {e}"
            )

    def _remove_unnecessary_fields(self, json_object):
        json_copy = json_object.copy()
        for k in json_copy.keys():
            if k in NON_EXTRACT_KEYS:
                del json_object[k]
        return json_object

    def _save_json(self, path, json_object):
        json_object = self._remove_unnecessary_fields(json_object)
        try:
            with open(path, "w") as json_file:
                json.dump(json_object, json_file, indent=4)
                json_file.write("\n")
        except OSError as e:
            raise AirbyteExtractorException(f"Couldn't write {path}: {e}")

    def _save_json_connection(self, connection: dict):
        connection = copy(connection)
        connection.pop("connectionId", None)

        connection_source_name = self._get_airbyte_source_from_id(connection["sourceId"])["name"]
        connection_destination_name = self._get_airbyte_destination_from_id(
            connection["destinationId"]
        )["name"]

        connection.pop("sourceId", None)
        connection.pop("destinationId", None)
        connection.pop("sourceCatalogId", None)
        connection["sourceName"] = connection_source_name
        connection["destinationName"] = connection_destination_name
        filename = self._normalize_filename(
            f"{connection_source_name}-{connection_destination_name}.json"
        )

        path = os.path.join(self.connections_extract_destination, filename)
        self._save_json(path, connection)
        self.extraction_results["connections"].add(filename)

    def _save_json_destination(self, destination):
        destination = copy(destination)
        destination.pop("destinationId", None)
        destination.pop("workspaceId", None)
        filename = f"{self._normalize_filename(destination['name'])}.json"
        path = os.path.join(self.destinations_extract_destination, filename)
        self._save_json(path, destination)
        self.extraction_results["destinations"].add(filename)

    def _save_json_source(self, source):
        source = copy(source)
        source.pop("sourceId", None)
        source.pop("workspaceId", None)
        filename = f"{self._normalize_filename(source['name'])}.json"
        path = os.path.join(self.sources_extract_destination, filename)
        self._save_json(path, source)
        self.extraction_results["sources"].add(filename)

    def get_config_value(self, key):
        return self.coves_config.integrated["extract"]["airbyte"][key]
