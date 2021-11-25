from rich.console import Console

from dbt_coves.tasks.base import BaseConfiguredTask
from dbt_coves.utils import shell

import requests, json, os, subprocess
from typing import Dict
from requests.exceptions import RequestException

console = Console()


class AirbyteApiCallerException(Exception):
    pass


class AirbyteExtractorException(Exception):
    pass


class AirbyteApiCaller:
    def api_call(self, endpoint: str, body: Dict[str, str] = None):
        """
        Generic `api caller` for contacting Airbyte
        """
        try:
            response = requests.post(endpoint, json=body)
            if response.status_code == 204:
                return response
            elif response.status_code >= 200 and response.status_code < 300:
                return json.loads(response.text)
            else:
                raise RequestException(
                    f"Unexpected status code from airbyte: {response.status_code}"
                )
        except RequestException as e:
            raise AirbyteApiCallerException("Airbyte API error: " + str(e))

    def __init__(self, api_host, api_port):
        try:
            airbyte_host = api_host
            airbyte_port = api_port
            airbyte_api_root = "api/v1/"
            airbyte_api_base_endpoint = f"http://{airbyte_host}:{airbyte_port}/{airbyte_api_root}"  # TODO: what can we do regarding 'http' or 'https' toggling?
        except ValueError as e:
            raise AirbyteApiCallerException(
                f"Error initializing Airbyte API Caller: Missing configuration: {e}"
            )

        airbyte_api_list_component = airbyte_api_base_endpoint + "{component}/list"
        self.airbyte_endpoint_list_connections = airbyte_api_list_component.format(
            component="connections"
        )
        self.airbyte_endpoint_list_sources = airbyte_api_list_component.format(
            component="sources"
        )
        self.airbyte_endpoint_list_destinations = airbyte_api_list_component.format(
            component="destinations"
        )

        airbyte_endpoint_list_workspaces = airbyte_api_list_component.format(
            component="workspaces"
        )

        airbyte_api_create_component = airbyte_api_base_endpoint + "{component}/create"
        self.airbyte_endpoint_create_connections = airbyte_api_create_component.format(
            component="connections"
        )
        self.airbyte_endpoint_create_sources = airbyte_api_create_component.format(
            component="sources"
        )
        self.airbyte_endpoint_create_destinations = airbyte_api_create_component.format(
            component="destinations"
        )

        airbyte_api_update_component = airbyte_api_base_endpoint + "{component}/update"
        self.airbyte_endpoint_update_sources = airbyte_api_update_component.format(
            component="sources"
        )
        self.airbyte_endpoint_update_destinations = airbyte_api_update_component.format(
            component="destinations"
        )
        self.airbyte_endpoint_delete_connection = (
            airbyte_api_base_endpoint + "connections/delete"
        )

        try:
            self.airbyte_workspace_id = self.api_call(airbyte_endpoint_list_workspaces)[
                "workspaces"
            ][0]["workspaceId"]
            self.standard_request_body = {"workspaceId": self.airbyte_workspace_id}
            self.airbyte_connections_list = self.api_call(
                self.airbyte_endpoint_list_connections, self.standard_request_body
            )["connections"]
            self.airbyte_sources_list = self.api_call(
                self.airbyte_endpoint_list_sources, self.standard_request_body
            )["sources"]
            self.airbyte_destinations_list = self.api_call(
                self.airbyte_endpoint_list_destinations, self.standard_request_body
            )["destinations"]
        except:
            raise AirbyteApiCallerException(
                "Couldn't retrieve Airbyte connections, sources and destinations"
            )


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
            "--to",
            type=str,
            required=True,
            help="Where json files will be generated, i.e. " "'airbyte'",
        )
        subparser.add_argument(
            "--host",
            type=str,
            required=True,
            help="Airbyte's API hostname, i.e. 'airbyte-server'",
        )
        subparser.add_argument(
            "--port",
            type=str,
            required=True,
            help="Airbyte's API port, i.e. '8001'",
        )

        # --host
        # --port
        subparser.set_defaults(cls=cls, which="airbyte")
        return subparser

    def run(self):
        extract_destination = self.get_config_value("to")
        airbyte_host = self.get_config_value("host")
        airbyte_port = self.get_config_value("port")

        extract_destination = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "load", "data")
        )
        self.connections_extract_destination = os.path.abspath(
            os.path.join(extract_destination, "connections")
        )
        self.destinations_extract_destination = os.path.abspath(
            os.path.join(extract_destination, "destinations")
        )
        self.sources_extract_destination = os.path.abspath(
            os.path.join(extract_destination, "sources")
        )

        self.airbyte_api_caller = AirbyteApiCaller(airbyte_host, airbyte_port)

        dbt_sources_list = self._shell_run(
            "dbt ls --resource-type source",
            "/home/bruno/dev/convexa/dbt-coves/dbt",
        )
        if dbt_sources_list:
            dbt_sources_list = self._remove_sources_prefix(dbt_sources_list)
            print(dbt_sources_list)
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
                        # filename = source.replace('.', '-') + ".json"
                        self._save_json_connection(source_connection)
                        self._save_json_destination(source_destination)
                        self._save_json_source(source_source)
                else:
                    print(f"There is no Airbyte Connection for source: {source}")
        else:
            raise AirbyteExtractorException("There are no DBT Sources compiled")
        return 0

    def _remove_sources_prefix(self, sources_list):
        return [
            source.lower().replace("source:", "").replace("_AIRBYTE_RAW_".lower(), "")
            for source in sources_list
        ]

    def _get_airbyte_connection_for_table(self, table):
        """
        Given a table name, returns the corresponding airbyte connection
        """
        for conn in self.airbyte_api_caller.airbyte_connections_list:
            for stream in conn["syncCatalog"]["streams"]:
                if stream["stream"]["name"].lower() == table:
                    return conn

    def _get_airbyte_destination_from_id(self, destinationId):
        """
        Get the complete Destination object from it's ID
        """
        for destination in self.airbyte_api_caller.airbyte_destinations_list:
            if destination["destinationId"] == destinationId:
                return destination
        raise AirbyteExtractorException(
            f"Airbyte error: there are no destinations for id {destinationId}"
        )

    def _get_airbyte_source_from_id(self, sourceId):
        """
        Get the complete Source object from it's ID
        """
        for source in self.airbyte_api_caller.airbyte_sources_list:
            if source["sourceId"] == sourceId:
                return source
        raise AirbyteExtractorException(
            f"Airbyte error: there are no sources for id {sourceId}"
        )

    def _save_json(self, path, object):
        with open(path, "w") as json_file:
            json.dump(object, json_file)

    def _save_json_connection(self, connection):
        connection_source_name = self._get_airbyte_source_from_id(
            connection["sourceId"]
        )["connectionConfiguration"]["dataset_name"]
        connection_destination_name = self._get_airbyte_destination_from_id(
            connection["destinationId"]
        )["name"]
        filename = f"{connection_source_name}-{connection_destination_name}.json"
        path = os.path.join(self.connections_extract_destination, filename.lower())
        self._save_json(path, connection)

    def _save_json_destination(self, destination):
        filename = f"{destination['name']}.json"
        path = os.path.join(self.destinations_extract_destination, filename.lower())
        self._save_json(path, destination)

    def _save_json_source(self, source):
        filename = f"{source['connectionConfiguration']['dataset_name']}.json"
        path = os.path.join(self.sources_extract_destination, filename.lower())
        self._save_json(path, source)

    def _shell_run(self, bash_cmd, cwd=None):
        """
        Run a given shell command, providing or not a CWD (Change Working Directory)
        Returns shell's `stdout`, or None if resulted in `stderr`
        """
        process = subprocess.run(
            bash_cmd.split(),
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        stdout = process.stdout.decode().strip()
        full_stdout = stdout.split("\n") if "\n" in stdout else [stdout]
        return [line for line in full_stdout if "source:" in line]

    def get_config_value(self, key):
        return self.coves_config.integrated["extract"]["airbyte"][key]
