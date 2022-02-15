from rich.console import Console

from dbt_coves.tasks.base import BaseConfiguredTask
from dbt_coves.utils import shell
from os import path
import json, os, pathlib, glob, re
from typing import Dict
from dbt_coves.utils.airbyte_api import AirbyteApiCaller, AirbyteApiCallerException

console = Console()


class AirbyteLoaderException(Exception):
    pass


class LoadAirbyteTask(BaseConfiguredTask):
    """
    Task that loads airbyte sources, connections and destinations and stores them as json files
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
            help="Where json files will be loaded from, i.e. " "'/var/data'",
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
            "--secrets",
            type=str,
            help="Secret files location for Airbyte configuration",
        )
        subparser.set_defaults(cls=cls, which="airbyte")
        return subparser

    def run(self):
        self.loading_results = {
            "sources": {"updated": [], "created": []},
            "destinations": {"updated": [], "created": []},
            "connections": {"updated": [], "created": []},
        }
        load_destination = self.get_config_value("path")
        airbyte_host = self.get_config_value("host")
        airbyte_port = self.get_config_value("port")
        secrets_path = self.get_config_value("secrets")

        path = pathlib.Path(load_destination)

        connections_path = path / "connections"
        connections_path.mkdir(parents=True, exist_ok=True)
        sources_path = path / "sources"
        sources_path.mkdir(parents=True, exist_ok=True)
        destinations_path = path / "destinations"
        destinations_path.mkdir(parents=True, exist_ok=True)

        self.secrets_path = os.path.abspath(secrets_path)
        self.connections_load_destination = os.path.abspath(connections_path)
        self.destinations_load_destination = os.path.abspath(destinations_path)
        self.sources_load_destination = os.path.abspath(sources_path)

        self.airbyte_api_caller = AirbyteApiCaller(airbyte_host, airbyte_port)
        self.airbyte_api_caller.load_definitions()

        console.print(
            f"Loading DBT Sources into Airbyte from {os.path.abspath(path)}\n"
        )

        # Look for dbt sources
        dbt_sources_list = shell.run_dbt_ls(
            "dbt ls --resource-type source",
            None,
        )
        if dbt_sources_list:
            dbt_sources_list = self._remove_sources_prefix(dbt_sources_list)
            for source in dbt_sources_list:
                # Obtain table_name from dbt's 'db.schema.table'
                source_table = [element.lower() for element in source.split(".")][2]

                # Which connection.json corresponds to that source
                connection_json = self._get_conn_json_for_source(source_table)

                if connection_json:
                    # Get it's source.json
                    source_json = self._get_src_json_by_source_name(
                        connection_json["sourceName"]
                    )
                    # Get it's destination.json
                    destination_json = self._get_dest_json_by_destination_name(
                        connection_json["destinationName"]
                    )
                    if source_json and destination_json:

                        source_id = self._create_or_update_source(source_json)
                        destination_id = self._create_or_update_destination(
                            destination_json
                        )
                        self._create_or_update_connection(
                            connection_json, source_table, source_id, destination_id
                        )

                    else:
                        # raise AirbyteLoaderException(f"There is no exported source-destination combination for connection {connection_json['connectionId']}")
                        print(
                            f"There is no exported source-destination combination for connection {connection_json['connectionId']}"
                        )
                else:
                    # raise AirbyteLoaderException(f"There is no exported Connection configuration for {source_table}")
                    print(
                        f"There is no exported Connection configuration for {source_table}"
                    )

            console.print(
                f"""Load results:\n
Sources:
    Created: {self.loading_results['sources']['created']}
    Updated: {self.loading_results['sources']['updated']}
Destinations:
    Created: {self.loading_results['destinations']['created']}
    Updated: {self.loading_results['destinations']['updated']}
Connections:
    Created: {self.loading_results['connections']['created']}
    Updated: {self.loading_results['connections']['updated']}
"""
            )
            return 0
        else:
            raise AirbyteLoaderException("There are no compiled DBT Sources")

    def _remove_sources_prefix(self, sources_list):
        return [source.lower().replace("_airbyte_raw_", "") for source in sources_list]

    def _retrieve_all_jsons_from_path(self, path):
        jsons = []
        for file in glob.glob(path + "/*.json"):
            with open(file, "r") as json_file:
                jsons.append(json.load(json_file))
        return jsons

    def _get_conn_json_for_source(self, table_name):
        for json_file in self._retrieve_all_jsons_from_path(
            self.connections_load_destination
        ):
            for stream in json_file["syncCatalog"]["streams"]:
                if stream["config"]["aliasName"].lower() == table_name:
                    return json_file

    def _get_src_json_by_source_name(self, source_name):
        for json_file in self._retrieve_all_jsons_from_path(
            self.sources_load_destination
        ):
            if json_file["name"].lower() == source_name:
                return json_file

    def _get_dest_json_by_destination_name(self, destination_name):
        for json_file in self._retrieve_all_jsons_from_path(
            self.destinations_load_destination
        ):
            if json_file["name"].lower() == destination_name:
                return json_file

    def _get_secrets(self, exported_json_data, directory):
        """
        Get Airbyte's connectionConfiguration keys and values for a specified filename (source.json) and directory or object type: destinations/sources
        """
        try:
            connection_configuration = exported_json_data["connectionConfiguration"]
            airbyte_object_type = directory[:-1]
            # Regex: any string that contains only wildcards from beginning to end
            wildcard_pattern = re.compile("^\*+$")
            wildcard_keys = [
                str(k)
                for k, v in connection_configuration.items()
                if wildcard_pattern.match(str(v))
            ]

            # Only look for needed secret files == only those who have wildcards in their configuration
            if wildcard_keys:
                # Get the secret file for that name
                secret_file = os.path.join(
                    self.secrets_path,
                    directory,
                    exported_json_data["name"].lower() + ".json",
                )

                if path.isfile(secret_file):
                    secret_data = json.load(open(secret_file))
                    for key, value in secret_data.items():
                        if key in wildcard_keys:
                            connection_configuration[key] = value
                            wildcard_keys.remove(key)
                    # If wildcard_keys is still not empty, there are missing key:values in secrets
                    if len(wildcard_keys) > 0:
                        raise AirbyteLoaderException(
                            f"The following keys are missing in [bold red]{secret_file}[/bold red] secret file: [bold red]{', '.join(k for k in wildcard_keys)}[/bold red]"
                        )
                    exported_json_data[
                        "connectionConfiguration"
                    ] = connection_configuration
                else:
                    raise AirbyteLoaderException(
                        f"Secret file not found\n"
                        f"Please create [bold red]{secret_file}[/bold red] with the following keys: [bold red]{', '.join(k for k in wildcard_keys)}[/bold red]"
                    )
            return exported_json_data
        except AirbyteLoaderException as e:
            raise AirbyteLoaderException(
                f"There was an error loading secret data for {airbyte_object_type} [bold red]{exported_json_data['name']}[/bold red]: {e}"
            )

    def _create_source(self, exported_json_data):
        # Grab password from secret
        exported_json_data = self._get_secrets(exported_json_data, "sources")
        exported_json_data["workspaceId"] = self.airbyte_api_caller.airbyte_workspace_id
        exported_json_data[
            "sourceDefinitionId"
        ] = self._get_source_definition_id_by_name(exported_json_data.pop("sourceName"))
        try:
            response = self.airbyte_api_caller.api_call(
                self.airbyte_api_caller.airbyte_endpoint_create_sources,
                exported_json_data,
            )
            self.airbyte_api_caller.airbyte_sources_list.append(response)
            self.loading_results["sources"]["created"].append(
                exported_json_data["name"]
            )
            return response["sourceId"]

        except:
            raise AirbyteApiCallerException("Could not create Airbyte Source")

    def _update_source(self, exported_json_data, source_id):
        exported_json_data["sourceId"] = source_id
        exported_json_data.pop("sourceName")

        exported_json_data = self._get_secrets(exported_json_data, "sources")
        try:
            response = self.airbyte_api_caller.api_call(
                self.airbyte_api_caller.airbyte_endpoint_update_sources,
                exported_json_data,
            )
            self.loading_results["sources"]["updated"].append(
                exported_json_data["name"]
            )
            return response["sourceId"]
        except KeyError:
            raise AirbyteLoaderException("Could not update source")

    def _create_or_update_source(self, exported_json_data):
        """
        Decide whether creating or updating an existing source (if it's name corresponds to an existing name in JSON exported configuration)
        """
        for src in self.airbyte_api_caller.airbyte_sources_list:
            if exported_json_data["name"] == src["name"]:
                return self._update_source(exported_json_data, src["sourceId"])

        return self._create_source(exported_json_data)

    def _get_destination_definition_id_by_name(self, destination_type_name):
        """
        Get destination definition ID by it's name (File, Postgres, Snowflake, BigQuery, MariaDB, etc)
        """
        for definition in self.airbyte_api_caller.destination_definitions:
            if definition["name"] == destination_type_name:
                return definition["destinationDefinitionId"]
        raise AirbyteLoaderException(
            f"There is no destination definition for {destination_type_name}. Please review Airbyte's configuration"
        )

    def _get_source_definition_id_by_name(self, source_type_name):
        """
        Get destination definition ID by it's name (File, Postgres, Snowflake, BigQuery, MariaDB, etc)
        """
        for definition in self.airbyte_api_caller.source_definitions:
            if definition["name"] == source_type_name:
                return definition["sourceDefinitionId"]
        raise AirbyteLoaderException(
            f"There is no source definition for {source_type_name}. Please review Airbyte's configuration"
        )

    def _create_destination(self, exported_json_data):
        exported_json_data = self._get_secrets(exported_json_data, "destinations")
        exported_json_data["workspaceId"] = self.airbyte_api_caller.airbyte_workspace_id
        exported_json_data[
            "destinationDefinitionId"
        ] = self._get_destination_definition_id_by_name(
            exported_json_data.pop("destinationName")
        )
        try:

            response = self.airbyte_api_caller.api_call(
                self.airbyte_api_caller.airbyte_endpoint_create_destinations,
                exported_json_data,
            )
            self.airbyte_api_caller.airbyte_destinations_list.append(response)
            self.loading_results["destinations"]["created"].append(
                exported_json_data["name"]
            )
            return response["destinationId"]
        except:
            raise AirbyteApiCallerException("Could not create Airbyte destination")

    def _update_destination(self, exported_json_data, destination_id):
        exported_json_data.pop("destinationName")
        exported_json_data["destinationId"] = destination_id

        exported_json_data = self._get_secrets(exported_json_data, "destinations")

        try:
            response = self.airbyte_api_caller.api_call(
                self.airbyte_api_caller.airbyte_endpoint_update_destinations,
                exported_json_data,
            )
            if (
                exported_json_data["name"]
                not in self.loading_results["destinations"]["updated"]
            ):
                self.loading_results["destinations"]["updated"].append(
                    exported_json_data["name"]
                )
            return response["destinationId"]
        except KeyError:
            raise AirbyteLoaderException("Could not update destination")

    def _create_or_update_destination(self, exported_json_data):
        """
        Decide whether creating or updating an existing destination (if it's name corresponds to an existing name in JSON exported configuration)
        """
        for destination in self.airbyte_api_caller.airbyte_destinations_list:
            if exported_json_data["name"] == destination["name"]:
                return self._update_destination(
                    exported_json_data, destination["destinationId"]
                )

        return self._create_destination(exported_json_data)

    def _create_connection(self, exported_json_data, source_id, destination_id):
        exported_json_data["sourceId"] = source_id
        exported_json_data["destinationId"] = destination_id
        connection_name = f"{exported_json_data['sourceName']}-{exported_json_data['destinationName']}"
        # The custom fields `sourceName` and `destinationName` (created by dbt-coves extract) must be popped (Airbyte's API responds they are unrecognized)
        exported_json_data.pop("sourceName")
        exported_json_data.pop("destinationName")

        try:
            response = self.airbyte_api_caller.api_call(
                self.airbyte_api_caller.airbyte_endpoint_create_connections,
                exported_json_data,
            )
            self.airbyte_api_caller.airbyte_connections_list.append(response)
            if "connectionId" in response:
                return connection_name
            else:
                raise AirbyteApiCallerException("Could not create Airbyte connection")
        except:
            raise AirbyteApiCallerException("Could not create Airbyte connection")

    def _delete_connection(self, connection_id):
        try:
            conn_delete_req_body = {"connectionId": connection_id}
            self.airbyte_api_caller.api_call(
                self.airbyte_api_caller.airbyte_endpoint_delete_connection,
                conn_delete_req_body,
            )
        except:
            raise AirbyteLoaderException(
                "Could not delete Airbyte connection for re-creation"
            )

    def _get_connection_id_by_table_name(self, table_name):
        """
        Given a table name, returns the corresponding airbyte connection
        """
        for conn in self.airbyte_api_caller.airbyte_connections_list:
            for stream in conn["syncCatalog"]["streams"]:
                if stream["stream"]["name"].lower() == table_name:
                    return conn["connectionId"]
        return False

    def _create_or_update_connection(
        self, exported_json_data, table_name, source_id, destination_id
    ):
        """
        Decide whether creating or updating an existing connection\n
        if exported_data -> stream_name exists in Airbyte Sources, it's for modifying
        """
        connection_id = self._get_connection_id_by_table_name(table_name)
        if connection_id:
            # Connection update
            self._delete_connection(connection_id)
            conn_name = self._create_connection(
                exported_json_data, source_id, destination_id
            )
            self.loading_results["connections"]["updated"].append(conn_name)
        else:
            # Connection creation
            conn_name = self._create_connection(
                exported_json_data, source_id, destination_id
            )
            self.loading_results["connections"]["created"].append(conn_name)

    def get_config_value(self, key):
        return self.coves_config.integrated["load"]["airbyte"][key]
