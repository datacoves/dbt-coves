from rich.console import Console

from dbt_coves.tasks.base import BaseConfiguredTask
from dbt_coves.tasks.generate import sources
from dbt_coves.utils import shell
from dbt_coves.utils.airbyte_api import AirbyteApiCaller

import requests, json, os, subprocess, pathlib
from typing import Dict
from requests.exceptions import RequestException
from copy import copy

# from dbt_coves.utils import airbyte_api

console = Console()


class AirbyteExtractorException(Exception):
    pass


class ExtractAirbyteTask(BaseConfiguredTask):
    """
    Task that extracts airbyte sources, connections and destinations and stores them as json files
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "airbyte",
            parents=[base_subparser],
            help="Extracts airbyte sources, connections and destinations and stores them as json files",
        )
        subparser.add_argument(
            "--path",
            type=str,
            help="Where json files will be generated, i.e. " "'airbyte'",
        )
        subparser.add_argument(
            "--host",
            type=str,
            help="Airbyte's API hostname, i.e. 'airbyte-server'",
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

        path = pathlib.Path(extract_destination)

        connections_path = path / "connections"
        connections_path.mkdir(parents=True, exist_ok=True)
        sources_path = path / "sources"
        sources_path.mkdir(parents=True, exist_ok=True)
        destinations_path = path / "destinations"
        destinations_path.mkdir(parents=True, exist_ok=True)

        self.connections_extract_destination = os.path.abspath(connections_path)
        self.destinations_extract_destination = os.path.abspath(destinations_path)
        self.sources_extract_destination = os.path.abspath(sources_path)

        self.airbyte_api_caller = AirbyteApiCaller(airbyte_host, airbyte_port)

        console.print(
            f"Extracting Airbyte's [b]Source[/b], [b]Destination[/b] and [b]Connection[/b] configurations to {os.path.abspath(path)}\n"
        )

        dbt_sources_list = shell.run_dbt_ls(
            "dbt ls --resource-type source",
            None,
        )
        if dbt_sources_list:
            dbt_sources_list = self._remove_airbyte_prefix(dbt_sources_list)
            for source in dbt_sources_list:
                # Obtain db.schema.table
                source_db, source_schema, source_table = [
                    element.lower() for element in source.split(".")
                ]

                source_connection = self._get_airbyte_connection_for_table(source_table)

                if source_connection:
                    source_destination = self._get_airbyte_destination_from_id(
                        source_connection["destinationId"]
                    )
                    source_source = self._get_airbyte_source_from_id(
                        source_connection["sourceId"]
                    )

                    if source_destination and source_source:
                        self._save_json_connection(source_connection)
                        self._save_json_destination(source_destination)
                        self._save_json_source(source_source)
                else:
                    print(f"There is no Airbyte Connection for source: {source}")
            console.print(
                "Extraction successful!\n"
                f"[u]Sources[/u]: {self.extraction_results['sources']}\n"
                f"[u]Destinations[/u]: {self.extraction_results['destinations']}\n"
                f"[u]Connections[/u]: {self.extraction_results['connections']}\n"
            )
            return 0
        else:
            raise AirbyteExtractorException("There are no dbt Sources compiled")

    def _remove_airbyte_prefix(self, sources_list):
        return [source.lower().replace("_airbyte_raw_", "") for source in sources_list]

    def _get_airbyte_connection_for_table(self, table):
        """
        Given a table name, returns the corresponding airbyte connection
        """
        for conn in self.airbyte_api_caller.airbyte_connections_list:
            for stream in conn["syncCatalog"]["streams"]:
                if stream["stream"]["name"].lower() == table:
                    return conn
        raise AirbyteExtractorException(
            f"Airbyte extract error: there are no connections for table {table}"
        )

    def _get_airbyte_destination_from_id(self, destinationId):
        """
        Get the complete Destination object from it's ID
        """
        for destination in self.airbyte_api_caller.airbyte_destinations_list:
            if destination["destinationId"] == destinationId:
                return destination
        raise AirbyteExtractorException(
            f"Airbyte extract error: there are no destinations for id {destinationId}"
        )

    def _get_airbyte_source_from_id(self, sourceId):
        """
        Get the complete Source object from it's ID
        """
        for source in self.airbyte_api_caller.airbyte_sources_list:
            if source["sourceId"] == sourceId:
                return source
        raise AirbyteExtractorException(
            f"Airbyte extract error: there are no sources for id {sourceId}"
        )

    def _save_json(self, path, object):
        with open(path, "w") as json_file:
            json.dump(object, json_file)

    def _save_json_connection(self, connection):
        connection = copy(connection)
        connection.pop("connectionId")

        connection_source_name = self._get_airbyte_source_from_id(
            connection["sourceId"]
        )["name"].lower()
        connection_destination_name = self._get_airbyte_destination_from_id(
            connection["destinationId"]
        )["name"].lower()

        # Once we used the source and destination IDs, they are no longer required and don't need to be saved
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
