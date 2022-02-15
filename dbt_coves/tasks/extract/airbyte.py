from rich.console import Console

from dbt_coves.tasks.base import BaseConfiguredTask
from dbt_coves.tasks.generate import sources
from dbt_coves.utils import shell
from dbt_coves.utils.airbyte_api import AirbyteApiCaller
from pathlib import Path

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
        subparser.add_argument(
            "--dbt_list_args",
            type=str,
            help="Extra dbt arguments, selectors or modifiers",
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
        dbt_modifiers = self.get_config_value("dbt_list_args")

        if not extract_destination or not airbyte_host or not airbyte_port:
            raise AirbyteExtractorException(
                f"Couldn't start extraction: one (or more) of the following arguments is missing either in the configuration file or Command-Line arguments: 'path', 'host', 'port'"
            )

        extract_destination = pathlib.Path(extract_destination)

        connections_path = extract_destination / "connections"
        sources_path = extract_destination / "sources"
        destinations_path = extract_destination / "destinations"

        self.connections_extract_destination = os.path.abspath(connections_path)
        self.destinations_extract_destination = os.path.abspath(destinations_path)
        self.sources_extract_destination = os.path.abspath(sources_path)

        self.airbyte_api_caller = AirbyteApiCaller(airbyte_host, airbyte_port)

        console.print(
            f"Extracting Airbyte's [b]Source[/b], [b]Destination[/b] and [b]Connection[/b] configurations to {os.path.abspath(extract_destination)}\n"
        )

        dbt_ls_cmd = f"dbt ls --resource-type source {dbt_modifiers}"

        try:
            dbt_sources_list = shell.run_dbt_ls(dbt_ls_cmd, None)
        except subprocess.CalledProcessError as e:
            raise AirbyteExtractorException(
                f"Command '{dbt_ls_cmd}' failed and returned with error (code {e.returncode})\n"
                f"Output: {e.output.decode('utf-8')}"
            )

        if not dbt_sources_list:
            raise AirbyteExtractorException(
                f"No compiled dbt sources found running '{dbt_ls_cmd}'"
            )

        manifest_json = json.load(open(Path() / "target" / "manifest.json"))
        dbt_sources_list = self._clean_sources_prefixes(dbt_sources_list)
        for source in dbt_sources_list:
            # Obtain db.schema.table
            source_db = manifest_json["sources"][source]["database"].lower()
            source_schema = manifest_json["sources"][source]["schema"].lower()
            source_table = (
                manifest_json["sources"][source]["identifier"]
                .lower()
                .replace("_airbyte_raw_", "")
            )

            source_connection = self._get_airbyte_connection_for_table(
                source_db, source_schema, source_table
            )
            if source_connection:
                source_destination = self._get_airbyte_destination_from_id(
                    source_connection["destinationId"]
                )
                source_source = self._get_airbyte_source_from_id(
                    source_connection["sourceId"]
                )

                if source_destination and source_source:
                    connections_path.mkdir(parents=True, exist_ok=True)
                    sources_path.mkdir(parents=True, exist_ok=True)
                    destinations_path.mkdir(parents=True, exist_ok=True)

                    self._save_json_connection(source_connection)
                    self._save_json_destination(source_destination)
                    self._save_json_source(source_source)
            else:
                console.print(
                    f"There is no Airbyte Connection for source: [red]{source}[/red]"
                )
        if len(self.extraction_results["connections"]) >= 1:
            console.print(
                f"Extraction to path {extract_destination} was successful!\n"
                f"[u]Sources[/u]: {self.extraction_results['sources']}\n"
                f"[u]Destinations[/u]: {self.extraction_results['destinations']}\n"
                f"[u]Connections[/u]: {self.extraction_results['connections']}\n"
            )
        else:
            console.print(f"No Airbyte Connections were extracted")
        return 0

    def _clean_sources_prefixes(self, sources_list):
        return [source.lower().replace("source:", "source.") for source in sources_list]

    def _get_connection_schema(self, conn, destination_config):
        """
        Given an airybte connection, returns a schema name
        """
        namespace_definition = conn["namespaceDefinition"]

        if namespace_definition == "source" or (
            conn["namespaceDefinition"] == "customformat"
            and conn["namespaceFormat"] == "${SOURCE_NAMESPACE}"
        ):
            source = self._get_airbyte_source_from_id(conn["sourceId"])
            if "schema" in source["connectionConfiguration"]:
                return source["connectionConfiguration"]["schema"].lower()
            else:
                return None
        elif namespace_definition == "destination":
            return destination_config["connectionConfiguration"]["schema"].lower()
        else:
            if namespace_definition == "customformat":
                return conn["namespaceFormat"]

    def _get_airbyte_connection_for_table(self, db, schema, table):
        """
        Given a table name, returns the corresponding airbyte connection
        """
        for conn in self.airbyte_api_caller.airbyte_connections_list:

            for stream in conn["syncCatalog"]["streams"]:
                if stream["stream"]["name"].lower() == table:
                    destination_config = self._get_airbyte_destination_from_id(
                        conn["destinationId"]
                    )
                    # match database
                    if (
                        db
                        == destination_config["connectionConfiguration"][
                            "database"
                        ].lower()
                    ):
                        airbyte_schema = self._get_connection_schema(
                            conn, destination_config
                        )
                        # and finally, match schema, if defined
                        if airbyte_schema == schema or not airbyte_schema:
                            return conn
        return None

    def _get_airbyte_destination_definition_from_id(self, definition_id):
        req_body = {"destinationDefinitionId": definition_id}
        return self.airbyte_api_caller.api_call(
            self.airbyte_api_caller.airbyte_endpoint_get_destination_definition,
            req_body,
        )

    def _get_airbyte_destination_from_id(self, destinationId):
        """
        Get the complete Destination object from it's ID
        """
        for destination in self.airbyte_api_caller.airbyte_destinations_list:
            if destination["destinationId"] == destinationId:
                # Grab Source definition ID
                destination_definition = (
                    self._get_airbyte_destination_definition_from_id(
                        destination["destinationDefinitionId"]
                    )
                )
                # Get Secret fields for source definition
                airbyte_secret_fields = self._get_airbyte_secret_fields_for_definition(
                    destination_definition
                )
                # Ensure all airbyte_secret fields are effectively hidden
                destination[
                    "connectionConfiguration"
                ] = self._hide_configuration_secret_fields(
                    destination["connectionConfiguration"], airbyte_secret_fields
                )

                return destination
        raise AirbyteExtractorException(
            f"Airbyte extract error: there is no Airbyte Destination for id [red]{destinationId}[/red]"
        )

    def _get_airbyte_source_definition_from_id(self, definition_id):
        req_body = {"sourceDefinitionId": definition_id}
        return self.airbyte_api_caller.api_call(
            self.airbyte_api_caller.airbyte_endpoint_get_source_definition, req_body
        )

    def _hide_configuration_secret_fields(
        self, connection_configuration, airbyte_secret_fields
    ):
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
                        if (
                            bool(definition["airbyte_secret"])
                            and dict_name not in secret_fields
                        ):
                            secret_fields.append(dict_name)
            return secret_fields
        except KeyError as e:
            raise AirbyteExtractorException(
                f"There was an error searching secret fields for {definition['connectionSpecification']['title']}"
            )

    def _get_airbyte_source_from_id(self, source_id):
        """
        Get the complete Source object from it's ID
        """
        for source in self.airbyte_api_caller.airbyte_sources_list:

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
                source[
                    "connectionConfiguration"
                ] = self._hide_configuration_secret_fields(
                    source["connectionConfiguration"], airbyte_secret_fields
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
