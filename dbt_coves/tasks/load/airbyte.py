import glob
import json
import os
import pathlib
from os import path

import requests
from rich.console import Console

from dbt_coves.utils.airbyte_api import AirbyteApiCaller, AirbyteApiCallerException

from .base import BaseLoadTask

console = Console()


class AirbyteLoaderException(Exception):
    pass


class LoadAirbyteTask(BaseLoadTask):
    """
    Task that loads airbyte sources, connections and destinations and stores them as json files
    """

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "airbyte",
            parents=[base_subparser],
            help="""Load airbyte sources, connections and destinations from JSON files,
            along with their secrets (if required)""",
        )
        subparser.add_argument(
            "--path",
            type=str,
            help="""Path where json files will be loaded from,
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
        subparser.add_argument(
            "--secrets-manager",
            type=str,
            help="Secret credentials provider, i.e. 'datacoves'",
        )
        subparser.add_argument("--secrets-url", type=str, help="Secret credentials provider url")
        subparser.add_argument(
            "--secrets-token", type=str, help="Secret credentials provider token"
        )
        subparser.add_argument(
            "--secrets-path",
            type=str,
            help="Secret files location for Airbyte configuration, i.e. './secrets'",
        )
        subparser.set_defaults(cls=cls, which="airbyte")
        return subparser

    def _load_secret_data(self) -> dict:
        # Contact the manager and retrieve Service Credentials
        secrets_url = os.getenv("DBT_COVES_SECRETS_URL") or self.get_config_value("secrets_url")
        secrets_token = os.getenv("DBT_COVES_SECRETS_TOKEN") or self.get_config_value(
            "secrets_token"
        )
        if not (secrets_url and secrets_token):
            raise AirbyteLoaderException(
                "[b]secrets_url[/b] and [b]secrets_token[/b] must be provided"
                "when using a Secrets Manager"
            )
        headers = {"Authorization": f"Token {secrets_token}"}
        response = requests.get(secrets_url, headers=headers)
        if response and response.status_code >= 200 and response.status_code < 300:
            return response.json()

    def run(self):
        self.loading_results = {
            "sources": {"updated": [], "created": []},
            "destinations": {"updated": [], "created": []},
            "connections": {"updated": [], "created": []},
        }
        self.load_destination = self.get_config_value("path")
        self.airbyte_host = self.get_config_value("host")
        self.airbyte_port = self.get_config_value("port")
        self.secrets_path = self.get_config_value("secrets_path")
        self.secrets_manager = self.get_config_value("secrets_manager")

        if not (self.airbyte_host and self.airbyte_port and self.load_destination):
            raise AirbyteLoaderException(
                "'path', 'host', and 'port' are required parameters in order to load Airbyte. "
                "Please refer to 'dbt-coves load airbyte --help' for more information."
            )
        if self.secrets_path and self.secrets_manager:
            raise AirbyteLoaderException(
                "Can't use 'secrets_path' and 'secrets_manager' simultaneously."
            )

        if self.secrets_manager:
            self.secrets_data = self._load_secret_data()

        if self.secrets_path:
            self.secrets_path = os.path.abspath(self.secrets_path)

        path = pathlib.Path(self.load_destination)
        if not path.exists():
            raise AirbyteLoaderException(
                f"Specified [b]load_destination[/b]: [u]{self.load_destination}[/u] does not exist"
            )

        self.connections_load_destination = os.path.abspath(path / "connections")
        self.destinations_load_destination = os.path.abspath(path / "destinations")
        self.sources_load_destination = os.path.abspath(path / "sources")

        self.airbyte_api_caller = AirbyteApiCaller(self.airbyte_host, self.airbyte_port)

        console.print(f"Loading DBT Sources into Airbyte from {os.path.abspath(path)}\n")

        # Load all exported
        extracted_sources = self.retrieve_all_jsons_from_path(self.sources_load_destination)
        # Create/update sources
        extracted_destinations = self.retrieve_all_jsons_from_path(
            self.destinations_load_destination
        )
        # Create/update destinations
        extracted_connections = self.retrieve_all_jsons_from_path(self.connections_load_destination)
        for source in extracted_sources:
            self._create_or_update_source(source)
        for destination in extracted_destinations:
            self._create_or_update_destination(destination)
        for connection in extracted_connections:
            self._create_or_update_connection(connection)

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

    def dbt_packages_exist(self, dbt_project_path):
        return glob.glob(f"{str(dbt_project_path)}/dbt_packages")

    def _remove_sources_prefix(self, sources_list):
        return [source.lower().replace("_airbyte_raw_", "") for source in sources_list]

    def _get_conn_json_for_source(self, table_name):
        for json_file in self._retrieve_all_jsons_from_path(self.connections_load_destination):
            for stream in json_file["syncCatalog"]["streams"]:
                if stream["config"]["aliasName"].lower() == table_name:
                    return json_file

    def _get_src_json_by_source_name(self, source_name):
        for json_file in self._retrieve_all_jsons_from_path(self.sources_load_destination):
            if json_file["name"].lower() == source_name:
                return json_file

    def _get_dest_json_by_destination_name(self, destination_name):
        for json_file in self._retrieve_all_jsons_from_path(self.destinations_load_destination):
            if json_file["name"].lower() == destination_name:
                return json_file

    def _get_secret_value_for_field(self, secret_data, field, secret_target_name):
        for k, v in secret_data.items():
            if k.lower() == field:
                return v
            if isinstance(v, dict):
                for kk, vv in v.items():
                    if kk.lower() == field:
                        return vv

        raise AirbyteLoaderException(
            f"Secret value missing for field [red]{field}[/red] in object {secret_target_name}"
        )

    def _update_connection_config_secret_fields(
        self,
        connection_configuration,
        wildcard_pattern,
        secret_data,
        secret_target_name,
    ):
        for k, v in connection_configuration.items():
            if wildcard_pattern == str(v):
                # Replace 'v' for secret value
                connection_configuration[k] = self._get_secret_value_for_field(
                    secret_data, k, secret_target_name
                )
            if isinstance(v, dict):
                self._update_connection_config_secret_fields(
                    v,
                    wildcard_pattern,
                    secret_data,
                    secret_target_name,
                )

    def _discover_hidden_keys(self, field, wildcard_pattern, fields_list):
        for k, v in field.items():
            if isinstance(v, dict):
                self._discover_hidden_keys(v, wildcard_pattern, fields_list)
            if v == wildcard_pattern:
                fields_list.append(k)
        return fields_list

    def _get_target_secret(self, secret_target_name):
        for secret in self.secrets_data:
            if secret["target"].lower() == secret_target_name:
                return secret
        return None

    def _get_secrets(self, exported_json_data, directory):
        """
        Get Airbyte's connectionConfiguration keys and values for a specified filename
        (source.json) and directory or object type: destinations/sources
        """
        try:
            connection_configuration = exported_json_data["connectionConfiguration"]
            secret_target_name = exported_json_data["name"].lower()
            # Regex: any string that contains only wildcards: from beginning to end
            wildcard_pattern = "**********"

            hidden_fields = list()
            for config_field, value in connection_configuration.items():
                if wildcard_pattern in str(value):
                    if isinstance(value, dict):
                        hidden_fields = self._discover_hidden_keys(
                            value, wildcard_pattern, hidden_fields
                        )
                    else:
                        hidden_fields.append(config_field)

            airbyte_object_type = directory[:-1]

            if hidden_fields:
                if self.secrets_manager and self.secrets_manager.lower() == "datacoves":
                    target_secret_data = self._get_target_secret(secret_target_name)

                    if not target_secret_data:
                        raise AirbyteLoaderException(
                            "Specified manager didn't provide secret information"
                            f"for[red]{secret_target_name}[/red]"
                        )
                    secret_data = target_secret_data["connection"]
                elif self.secrets_path:
                    wildcard_keys = [
                        str(k)
                        for k, v in connection_configuration.items()
                        if wildcard_pattern == str(v)
                    ]
                    secret_target_name = exported_json_data["name"].lower()
                    # Get the secret file for that name
                    secret_file = os.path.join(
                        self.secrets_path,
                        directory,
                        secret_target_name + ".json",
                    )

                    if path.isfile(secret_file):
                        secret_data = json.load(open(secret_file))
                    else:
                        raise AirbyteLoaderException(
                            f"Secret file for {secret_target_name} not found\n"
                            f"Please create secret for [bold red]{secret_target_name}[/bold red]"
                            "with the following keys:"
                            f"[bold red]{', '.join(k for k in wildcard_keys)}[/bold red]"
                        )
                else:
                    raise AirbyteLoaderException(
                        "secrets_path or secrets_manager flag must be provided"
                    )

                connection_configuration = self._update_connection_config_secret_fields(
                    connection_configuration,
                    wildcard_pattern,
                    secret_data,
                    secret_target_name,
                )
            return exported_json_data
        except AirbyteLoaderException as e:
            raise AirbyteLoaderException(
                f"There was an error loading secret data for {airbyte_object_type}"
                f"[bold red]{exported_json_data['name']}[/bold red]: {e}"
            )

    def _create_source(self, exported_json_data):
        exported_json_data.pop("connectorVersion")
        # Grab password from secret
        exported_json_data = self._get_secrets(exported_json_data, "sources")
        exported_json_data["workspaceId"] = self.airbyte_api_caller.airbyte_workspace_id
        exported_json_data["sourceDefinitionId"] = self._get_source_definition_id_by_name(
            exported_json_data.pop("sourceName")
        )
        try:
            response = self.airbyte_api_caller.api_call(
                self.airbyte_api_caller.airbyte_endpoint_create_sources,
                exported_json_data,
            )
            self.airbyte_api_caller.airbyte_sources_list.append(response)
            self.loading_results["sources"]["created"].append(exported_json_data["name"])
            return response["sourceId"]

        except AirbyteApiCallerException as e:
            raise AirbyteLoaderException(f"Could not create Airbyte Source: {e}")

    def _update_source(self, exported_json_data, source_id):
        exported_json_data["sourceId"] = source_id
        exported_json_data.pop("sourceName")
        exported_json_data.pop("connectorVersion")

        exported_json_data = self._get_secrets(exported_json_data, "sources")
        try:
            response = self.airbyte_api_caller.api_call(
                self.airbyte_api_caller.airbyte_endpoint_update_sources,
                exported_json_data,
            )
            self._add_update_result("sources", exported_json_data["name"])
            return response["sourceId"]
        except KeyError:
            raise AirbyteLoaderException("Could not update source")

    def _create_or_update_source(self, exported_json_data):
        """
        Decide whether creating or updating an existing source\
        (if it's name corresponds to an existing name in JSON exported configuration)
        """
        self._connector_versions_mismatch(exported_json_data, "source")

        for src in self.airbyte_api_caller.airbyte_sources_list:
            if exported_json_data["name"] == src["name"]:
                return self._update_source(exported_json_data, src["sourceId"])

        return self._create_source(exported_json_data)

    def _get_destination_definition_id_by_name(self, destination_type_name):
        """
        Get destination definition ID by it's name
        (File, Postgres, Snowflake, BigQuery, MariaDB, etc)
        """
        for definition in self.airbyte_api_caller.destination_definitions:
            if definition["name"] == destination_type_name:
                return definition["destinationDefinitionId"]
        raise AirbyteLoaderException(
            f"There is no destination definition for {destination_type_name}."
            "Please review Airbyte's configuration"
        )

    def _get_destination_definition_by_name(self, destination_type_name):
        """
        Get destination definition ID by it's name
        (File, Postgres, Snowflake, BigQuery, MariaDB, etc)
        """
        for definition in self.airbyte_api_caller.destination_definitions:
            if definition["name"] == destination_type_name:
                return definition
        raise AirbyteLoaderException(
            f"There is no destination definition for {destination_type_name}."
            "Please review Airbyte's configuration"
        )

    def _get_source_definition_id_by_name(self, source_type_name):
        """
        Get destination definition ID by it's name
        (File, Postgres, Snowflake, BigQuery, MariaDB, etc)
        """
        for definition in self.airbyte_api_caller.source_definitions:
            if definition["name"] == source_type_name:
                return definition["sourceDefinitionId"]
        raise AirbyteLoaderException(
            f"There is no source definition for {source_type_name}."
            "Please review Airbyte's configuration"
        )

    def _get_source_definition_by_name(self, source_type_name):
        """
        Get destination definition ID by it's name
        (File, Postgres, Snowflake, BigQuery, MariaDB, etc)
        """
        for definition in self.airbyte_api_caller.source_definitions:
            if definition["name"] == source_type_name:
                return definition
        raise AirbyteLoaderException(
            f"There is no source definition for {source_type_name}."
            "Please review Airbyte's configuration"
        )

    def _create_destination(self, exported_data):
        exported_data.pop("connectorVersion")
        exported_data = self._get_secrets(exported_data, "destinations")
        exported_data["workspaceId"] = self.airbyte_api_caller.airbyte_workspace_id
        exported_data["destinationDefinitionId"] = self._get_destination_definition_id_by_name(
            exported_data.pop("destinationName")
        )
        try:
            response = self.airbyte_api_caller.api_call(
                self.airbyte_api_caller.airbyte_endpoint_create_destinations,
                exported_data,
            )
            self.airbyte_api_caller.airbyte_destinations_list.append(response)
            self.loading_results["destinations"]["created"].append(exported_data["name"])
            return response["destinationId"]
        except AirbyteApiCallerException:
            raise AirbyteApiCallerException("Could not create Airbyte destination")

    def _update_destination(self, exported_json_data, destination_id):
        exported_json_data.pop("destinationName")
        exported_json_data["destinationId"] = destination_id
        exported_json_data.pop("connectorVersion")

        exported_json_data = self._get_secrets(exported_json_data, "destinations")

        try:
            response = self.airbyte_api_caller.api_call(
                self.airbyte_api_caller.airbyte_endpoint_update_destinations,
                exported_json_data,
            )
            self._add_update_result("destinations", exported_json_data["name"])
            return response["destinationId"]
        except KeyError:
            raise AirbyteLoaderException("Could not update destination")

    def _create_or_update_destination(self, exported_json_data):
        """
        Decide whether creating or updating an existing destination
        (if it's name corresponds to an existing name in JSON exported configuration)
        """
        self._connector_versions_mismatch(exported_json_data, "destination")

        for destination in self.airbyte_api_caller.airbyte_destinations_list:
            if exported_json_data["name"] == destination["name"]:
                return self._update_destination(exported_json_data, destination["destinationId"])

        return self._create_destination(exported_json_data)

    def _create_connection(self, exported_json_data, source_id, destination_id):
        exported_json_data["sourceId"] = source_id
        exported_json_data["destinationId"] = destination_id
        connection_name = (
            f"{exported_json_data['sourceName']}-{exported_json_data['destinationName']}"
        )
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
        except AirbyteApiCallerException:
            raise AirbyteApiCallerException("Could not create Airbyte connection")

    def _delete_connection(self, connection_id):
        try:
            conn_delete_req_body = {"connectionId": connection_id}
            self.airbyte_api_caller.api_call(
                self.airbyte_api_caller.airbyte_endpoint_delete_connection,
                conn_delete_req_body,
            )
        except AirbyteApiCallerException:
            raise AirbyteLoaderException("Could not delete Airbyte connection for re-creation")

    def _get_connection_id_by_table_name(self, table_name):
        """
        Given a table name, returns the corresponding airbyte connection
        """
        for conn in self.airbyte_api_caller.airbyte_connections_list:
            for stream in conn["syncCatalog"]["streams"]:
                if stream["stream"]["name"].lower() == table_name:
                    return conn["connectionId"]
        return False

    def _get_source_id_by_name(self, source_name):
        for source in self.airbyte_api_caller.airbyte_sources_list:
            if source["name"].lower() == source_name:
                return source["sourceId"]

    def _get_destination_id_by_name(self, destination_name):
        for destination in self.airbyte_api_caller.airbyte_destinations_list:
            if destination["name"].lower() == destination_name:
                return destination["destinationId"]

    def _get_connection_id_by_endpoints(self, source_id, destination_id):
        for connection in self.airbyte_api_caller.airbyte_connections_list:
            if (
                connection["sourceId"] == source_id
                and connection["destinationId"] == destination_id
            ):
                return connection["connectionId"]
        pass

    def _create_or_update_connection(self, connection_json):
        """
        Identify source_id and destination_id by their names
        Update or create connection
        """
        source_name = connection_json["sourceName"]
        destination_name = connection_json["destinationName"]
        source_id = self._get_source_id_by_name(source_name)
        destination_id = self._get_destination_id_by_name(destination_name)
        if not source_id or not destination_id:
            console.print(
                f"No existent source-destination pair found for {connection_json['name']}"
            )
            return
        connection_id = self._get_connection_id_by_endpoints(source_id, destination_id)
        if connection_id:
            # Connection update
            self._delete_connection(connection_id)
            conn_name = self._create_connection(connection_json, source_id, destination_id)
            self._add_update_result("connections", conn_name)
        else:
            # Connection creation
            conn_name = self._create_connection(connection_json, source_id, destination_id)
            self.loading_results["connections"]["created"].append(conn_name)

    # def _create_or_update_connection(
    #     self, exported_json_data, table_name, source_id, destination_id
    # ):
    #     """
    #     Decide whether creating or updating an existing connection\n
    #     if exported_data -> stream_name exists in Airbyte Sources, it's for modifying
    #     """
    #     connection_id = self._get_connection_id_by_table_name(table_name)
    #     if connection_id:
    #         # Connection update
    #         self._delete_connection(connection_id)
    #         conn_name = self._create_connection(
    #             exported_json_data, source_id, destination_id
    #         )
    #         self._add_update_result("connections", conn_name)
    #     else:
    #         # Connection creation
    #         conn_name = self._create_connection(
    #             exported_json_data, source_id, destination_id
    #         )
    #         self.loading_results["connections"]["created"].append(conn_name)

    def _add_update_result(self, obj_type, obj_name):
        if (obj_name not in self.loading_results[obj_type]["updated"]) and (
            obj_name not in self.loading_results[obj_type]["created"]
        ):
            self.loading_results[obj_type]["updated"].append(obj_name)

    def _connector_versions_mismatch(self, exported_json_data, object_type):
        if object_type == "source":
            object_definition = self._get_source_definition_by_name(
                exported_json_data["sourceName"]
            )
        if object_type == "destination":
            object_definition = self._get_destination_definition_by_name(
                exported_json_data["destinationName"]
            )

        if exported_json_data["connectorVersion"] != object_definition["dockerImageTag"]:
            console.print(
                f"[red]WARNING:[/red] Current Airbyte [b]{object_definition['name']}[/b]"
                f"{object_type} connector version [b]({object_definition['dockerImageTag']})"
                f"[/b] doesn't match exported [b]{exported_json_data['name']}[/b]"
                f"version ({exported_json_data['connectorVersion']}) being loaded"
            )

    def get_config_value(self, key):
        return self.coves_config.integrated["load"]["airbyte"][key]
