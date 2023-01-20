import glob
import json
import os
import pathlib
from copy import copy

from rich.console import Console

from dbt_coves.utils.api_caller import AirbyteApiCaller

from .base import BaseExtractTask

# from dbt_coves.utils import airbyte_api

console = Console()


class AirbyteExtractorException(Exception):
    pass


class ExtractAirbyteTask(BaseExtractTask):
    """
    Task that extracts airbyte sources, connections and destinations and stores them as json files
    """

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
            help="Airbyte's API hostname, i.e. 'http://airbyte-server'",
        )
        subparser.add_argument(
            "--port",
            type=str,
            help="Airbyte's API port, i.e. '8001'",
        )
        subparser.set_defaults(cls=cls, which="airbyte")
        return subparser

    def run(self):
        self.extraction_results = {
            "sources": set(),
            "destinations": set(),
            "connections": set(),
        }

        extract_destination = self.get_config_value("path")
        airbyte_host = self.get_config_value("host")
        airbyte_port = self.get_config_value("port")

        if not extract_destination or not airbyte_host or not airbyte_port:
            raise AirbyteExtractorException(
                "Couldn't start extraction: one (or more) of the following arguments is missing"
                "either in the configuration file or Command-Line arguments: 'path', 'host', 'port'"
            )

        extract_destination = pathlib.Path(extract_destination)

        connections_path = extract_destination / "connections"
        sources_path = extract_destination / "sources"
        destinations_path = extract_destination / "destinations"

        self.connections_extract_destination = os.path.abspath(connections_path)
        self.destinations_extract_destination = os.path.abspath(destinations_path)
        self.sources_extract_destination = os.path.abspath(sources_path)

        self.airbyte_api = AirbyteApiCaller(airbyte_host, airbyte_port)

        console.print(
            "Extracting Airbyte's [b]Source[/b], [b]Destination[/b] and [b]Connection[/b]"
            f"configurations to {os.path.abspath(extract_destination)}\n"
        )

        sources_path.mkdir(exist_ok=True, parents=True)
        destinations_path.mkdir(exist_ok=True, parents=True)
        connections_path.mkdir(exist_ok=True, parents=True)
        for airbyte_source in self.airbyte_api.airbyte_sources_list:
            source_id = airbyte_source["sourceId"]
            source_json = self._get_airbyte_source_from_id(source_id)
            self._save_json_source(source_json)

        for airbyte_destination in self.airbyte_api.airbyte_destinations_list:
            destination_id = airbyte_destination["destinationId"]
            destination_json = self._get_airbyte_destination_from_id(destination_id)
            self._save_json_destination(destination_json)

        for airbyte_conn in self.airbyte_api.airbyte_connections_list:
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

    def _get_airbyte_destination_definition_from_id(self, definition_id):
        req_body = {
            "destinationDefinitionId": definition_id,
            "workspaceId": self.airbyte_api.airbyte_workspace_id,
        }
        return self.airbyte_api.api_call(
            self.airbyte_api.airbyte_endpoint_get_destination_definition,
            req_body,
        )

    def _get_airbyte_destination_from_id(self, destinationId):
        """
        Get the complete Destination object from it's ID
        """
        for destination in self.airbyte_api.airbyte_destinations_list:
            if destination["destinationId"] == destinationId:
                # Grab Source definition ID
                destination_definition = self._get_airbyte_destination_definition_from_id(
                    destination["destinationDefinitionId"]
                )
                # Get Secret fields for source definition
                airbyte_secret_fields = self._get_airbyte_secret_fields_for_definition(
                    destination_definition
                )
                # Ensure all airbyte_secret fields are effectively hidden
                destination["connectionConfiguration"] = self._hide_configuration_secret_fields(
                    destination["connectionConfiguration"], airbyte_secret_fields
                )

                # Add object definition version
                destination["connectorVersion"] = self._get_connector_version(
                    "destinationDefinitionId",
                    self.airbyte_api.destination_definitions,
                    destination_definition["destinationDefinitionId"],
                )

                return destination
        raise AirbyteExtractorException(
            "Airbyte extract error: there is no Airbyte"
            f"Destination for id [red]{destinationId}[/red]"
        )

    def _get_connector_version(self, lookup_field, definitions_list, definition_id):
        for definition in definitions_list:
            if definition[lookup_field] == definition_id:
                return definition["dockerImageTag"]
        raise AirbyteExtractorException(f"No connector definition found for ID {definition_id}")

    def _get_airbyte_source_definition_from_id(self, definition_id):
        req_body = {
            "sourceDefinitionId": definition_id,
            "workspaceId": self.airbyte_api.airbyte_workspace_id,
        }
        return self.airbyte_api.api_call(
            self.airbyte_api.airbyte_endpoint_get_source_definition, req_body
        )

    def _hide_configuration_secret_fields(self, connection_configuration, airbyte_secret_fields):
        for k, v in connection_configuration.items():
            if isinstance(v, dict):
                self._hide_configuration_secret_fields(v, airbyte_secret_fields)
            elif k in airbyte_secret_fields:
                connection_configuration[k] = "**********"
        return connection_configuration

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
                "There was an error searching secret fields for"
                f"{definition['connectionSpecification']['title']}:"
                f"{e}"
            )

    def _get_airbyte_source_from_id(self, source_id):
        """
        Get the complete Source object from it's ID
        """
        for source in self.airbyte_api.airbyte_sources_list:
            if source["sourceId"] == source_id:
                # Grab Source definition ID
                source_definition = self._get_airbyte_source_definition_from_id(
                    source["sourceDefinitionId"]
                )
                # Get Secret fields for source definition
                airbyte_secret_fields = self._get_airbyte_secret_fields_for_definition(
                    source_definition
                )
                # Ensure all airbyte_secret fields are effectively hidden
                source["connectionConfiguration"] = self._hide_configuration_secret_fields(
                    source["connectionConfiguration"], airbyte_secret_fields
                )

                # Add object definition version
                source["connectorVersion"] = self._get_connector_version(
                    "sourceDefinitionId",
                    self.airbyte_api.source_definitions,
                    source_definition["sourceDefinitionId"],
                )

                return source
        raise AirbyteExtractorException(
            f"Airbyte extract error: there is no Airbyte Source for id [red]{source_id}[/red]"
        )

    def _save_json(self, path, object):
        try:
            with open(path, "w") as json_file:
                json.dump(object, json_file, indent=4)
        except OSError as e:
            raise AirbyteExtractorException(f"Couldn't write {path}: {e}")

    def _save_json_connection(self, connection):
        connection = copy(connection)
        connection.pop("connectionId")

        connection_source_name = self._get_airbyte_source_from_id(connection["sourceId"])[
            "name"
        ].lower()
        connection_destination_name = self._get_airbyte_destination_from_id(
            connection["destinationId"]
        )["name"].lower()

        # Once we used the source and destination IDs,
        # they are no longer required and don't need to be saved
        # Instead, they are replaced with their respective names
        connection.pop("sourceId", None)
        connection.pop("destinationId", None)
        connection["sourceName"] = connection_source_name
        connection["destinationName"] = connection_destination_name
        filename = f"{connection_source_name}-{connection_destination_name}.json"
        path = os.path.join(self.connections_extract_destination, filename)

        self._save_json(path, connection)
        self.extraction_results["connections"].add(filename)

    def _save_json_destination(self, destination):
        destination = copy(destination)

        destination.pop("destinationDefinitionId", None)
        destination.pop("workspaceId", None)
        destination.pop("destinationId", None)
        filename = f"{destination['name']}.json"
        path = os.path.join(self.destinations_extract_destination, filename.lower())

        self._save_json(path, destination)
        self.extraction_results["destinations"].add(filename.lower())

    def _save_json_source(self, source):
        source = copy(source)
        source.pop("sourceDefinitionId", None)
        source.pop("workspaceId", None)
        source.pop("sourceId", None)
        filename = f"{source['name']}.json"
        path = os.path.join(self.sources_extract_destination, filename.lower())

        self._save_json(path, source)
        self.extraction_results["sources"].add(filename.lower())

    def get_config_value(self, key):
        return self.coves_config.integrated["extract"]["airbyte"][key]
