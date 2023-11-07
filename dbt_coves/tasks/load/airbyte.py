import json
import os
import pathlib
from copy import copy

import questionary
from rich.console import Console
from slugify import slugify

from dbt_coves.utils.api_caller import AirbyteApiCaller, AirbyteApiCallerException
from dbt_coves.utils.secrets import load_secret_manager_data
from dbt_coves.utils.tracking import trackable

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
            "--secrets-path",
            type=str,
            help="Secret files location for Airbyte configuration, i.e. './secrets'",
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
        subparser.add_argument("--secrets-project", type=str, help="Secret credentials project")
        subparser.add_argument("--secrets-tags", type=str, help="Secret credentials tags")
        subparser.add_argument("--secrets-key", type=str, help="Secret credentials key")
        subparser.set_defaults(cls=cls, which="airbyte")
        return subparser

    @trackable
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
            self.secrets_data = load_secret_manager_data(self)

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

        self.airbyte_api = AirbyteApiCaller(self.airbyte_host, self.airbyte_port)

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

        self._print_load_results()
        return 0

    def _print_load_results(self):
        """
        Inform the user successful loading results
        """
        console.print("[green][b]Load successful :heavy_check_mark:[/b][/green]")
        for obj_type, result_dict in self.loading_results.items():
            for action, results in result_dict.items():
                if len(results) > 0:
                    console.print(
                        f"[green]{obj_type.capitalize()} {action}:[/green] {', '.join(results)}"
                    )

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
            if secret["slug"].lower() == secret_target_name:
                return secret
        return None

    def _get_secrets(self, exported_json_data, directory):
        """
        Get Airbyte's connectionConfiguration keys and values for a specified filename
        (source.json) and directory or object type: destinations/sources
        """
        try:
            connection_configuration = exported_json_data["connectionConfiguration"]
            secret_target_name = slugify(exported_json_data["name"].lower())
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
                    secret_data = target_secret_data["value"]
                elif self.secrets_path:
                    secret_target_name = slugify(exported_json_data["name"].lower())
                    # Get the secret file for that name
                    expected_secret_filepath = pathlib.Path(
                        self.secrets_path, directory, secret_target_name + ".json"
                    )

                    if not expected_secret_filepath.exists():
                        raise AirbyteLoaderException(
                            f"Secret file {expected_secret_filepath} not found\n"
                            f"Please create secret file with the following keys: "
                            f"[bold red]{', '.join(k for k in hidden_fields)}[/bold red]"
                        )

                    secret_data = json.load(open(expected_secret_filepath))
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
                f"There was an error loading secret data for {airbyte_object_type} "
                f"[bold red]{exported_json_data['name']}[/bold red]: {e}"
            )

    def _update_source_or_destination(self, exported_data, object_type):
        update = True
        conn_status = self._test_update_connection(exported_data, object_type)
        if conn_status.get("status", "").lower() != "succeeded":
            console.print(
                f"Connection test for {exported_data['name']} failed:\n {conn_status['message']}"
            )
            update = questionary.confirm("Would you like to continue updating it?").ask()
        if update:
            response = self.airbyte_api.api_call(
                self.airbyte_api.api_endpoints["UPDATE_OBJECT"].format(obj=object_type),
                exported_data,
            )

            self._add_update_result(object_type, exported_data["name"])
            return response.get("sourceId", response.get("destinationId"))

    def _create_source_or_destination(self, exported_data, object_type):
        create = True
        response: dict = self.airbyte_api.api_call(
            self.airbyte_api.api_endpoints["CREATE_OBJECT"].format(obj=object_type),
            exported_data,
        )
        new_object_body = {}
        if object_type == "sources":
            new_object_body = {"sourceId": response["sourceId"]}
        elif object_type == "destinations":
            new_object_body = {"destinationId": response["destinationId"]}

        object_name = response.get("name", "")
        conn_status = self._test_created_object(new_object_body, object_type, object_name)
        if conn_status.get("status", "").lower() != "succeeded":
            console.print(
                f"Connection test for {exported_data['name']} failed:\n {conn_status['message']}"
            )
            create = questionary.confirm("Would you like to continue creating it?").ask()
        if create:
            self.airbyte_api.airbyte_destinations_list.append(response)
            self.loading_results[object_type]["created"].append(exported_data["name"])
            return response[next(iter(new_object_body))]
        else:
            self.airbyte_api.api_call(
                self.airbyte_api.api_endpoints["DELETE_OBJECT"].format(obj=object_type),
                new_object_body,
            )

    def _create_source(self, exported_data):
        exported_data.pop("connectorVersion")
        exported_data = self._get_secrets(exported_data, "sources")
        exported_data["workspaceId"] = self.airbyte_api.airbyte_workspace_id
        exported_data["sourceDefinitionId"] = self._get_source_definition_id_by_name(
            exported_data.pop("sourceName")
        )
        self._create_source_or_destination(exported_data, "sources")

    def _test_update_connection(self, data, obj_type):
        console.print(f"Testing update for [yellow]{data.get('name', 'object')}[/yellow]")
        return self.airbyte_api.api_call(
            self.airbyte_api.api_endpoints["TEST_UPDATE"].format(obj=obj_type), data, timeout=30
        )

    def _update_source(self, exported_data, source_id):
        exported_data["sourceId"] = source_id
        exported_data.pop("sourceName")
        exported_data.pop("connectorVersion")

        exported_data = self._get_secrets(exported_data, "sources")
        self._update_source_or_destination(exported_data, "sources")

    def _sources_are_equivalent(self, exported_source, current_source):
        current_source_copy = copy(current_source)
        exported_source_copy = copy(exported_source)
        exported_source_copy.pop("connectorVersion")
        current_source_copy.pop("icon")
        current_source_copy.pop("sourceId")
        current_source_copy.pop("sourceDefinitionId")
        current_source_copy.pop("workspaceId")
        return current_source_copy == exported_source_copy

    def _create_or_update_source(self, exported_json_data):
        """
        Decide whether creating or updating an existing source\
        (if it's name corresponds to an existing name in JSON exported configuration)
        """
        self._connector_versions_mismatch(exported_json_data, "source")

        for src in self.airbyte_api.airbyte_sources_list:
            if exported_json_data["name"] == src["name"]:
                if self._sources_are_equivalent(exported_json_data, src):
                    console.print(
                        f"Source [green]{src['name']}[/green] already up to date. Skipping"
                    )
                    return
                else:
                    return self._update_source(exported_json_data, src["sourceId"])

        return self._create_source(exported_json_data)

    def _get_destination_definition_id_by_name(self, destination_type_name):
        """
        Get destination definition ID by it's name
        (File, Postgres, Snowflake, BigQuery, MariaDB, etc)
        """
        for definition in self.airbyte_api.destination_definitions:
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
        for definition in self.airbyte_api.destination_definitions:
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
        for definition in self.airbyte_api.source_definitions:
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
        for definition in self.airbyte_api.source_definitions:
            if definition["name"] == source_type_name:
                return definition
        raise AirbyteLoaderException(
            f"There is no source definition for {source_type_name}."
            "Please review Airbyte's configuration"
        )

    def _test_created_object(self, test_body, obj_type, obj_name):
        console.print(f"Testing created {obj_type} [green]{obj_name}[/green]")
        return self.airbyte_api.api_call(
            self.airbyte_api.api_endpoints["TEST_CONNECTION"].format(obj=obj_type),
            test_body,
            timeout=30,
        )

    def _create_destination(self, exported_data):
        exported_data.pop("connectorVersion")
        exported_data = self._get_secrets(exported_data, "destinations")
        exported_data["workspaceId"] = self.airbyte_api.airbyte_workspace_id
        exported_data["destinationDefinitionId"] = self._get_destination_definition_id_by_name(
            exported_data.pop("destinationName")
        )
        self._create_source_or_destination(exported_data, "destinations")

    def _update_destination(self, exported_data, destination_id):
        exported_data.pop("destinationName")
        exported_data["destinationId"] = destination_id
        exported_data.pop("connectorVersion")

        exported_data = self._get_secrets(exported_data, "destinations")
        self._update_source_or_destination(exported_data, "destinations")

    def _destinations_are_equivalent(self, exported_destination, current_destination):
        current_destination_copy = copy(current_destination)
        exported_destination_copy = copy(exported_destination)
        exported_destination_copy.pop("connectorVersion")
        current_destination_copy.pop("destinationDefinitionId")
        current_destination_copy.pop("destinationId")
        current_destination_copy.pop("workspaceId")
        current_destination_copy.pop("icon")
        return current_destination_copy == exported_destination_copy

    def _create_or_update_destination(self, exported_json_data):
        """
        Decide whether creating or updating an existing destination
        (if it's name corresponds to an existing name in JSON exported configuration)
        """
        self._connector_versions_mismatch(exported_json_data, "destination")

        for destination in self.airbyte_api.airbyte_destinations_list:
            if exported_json_data["name"] == destination["name"]:
                if self._destinations_are_equivalent(exported_json_data, destination):
                    console.print(
                        f"Destination [green]{destination['name']}[/green] already up to date. Skipping"
                    )
                    return
                else:
                    return self._update_destination(
                        exported_json_data, destination["destinationId"]
                    )

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
            response = self.airbyte_api.api_call(
                self.airbyte_api.api_endpoints["CREATE_OBJECT"].format(obj="connections"),
                exported_json_data,
            )
            if response:
                self.airbyte_api.airbyte_connections_list.append(response)
                return connection_name
        except AirbyteApiCallerException as ex:
            raise AirbyteApiCallerException(f"Could not create Airbyte connection: {ex}")

    def _delete_connection(self, connection_id):
        try:
            conn_delete_req_body = {"connectionId": connection_id}
            self.airbyte_api.api_call(
                self.airbyte_api.api_endpoints["DELETE_OBJECT"].format(obj="connections"),
                conn_delete_req_body,
            )
        except AirbyteApiCallerException:
            raise AirbyteLoaderException("Could not delete Airbyte connection for re-creation")

    def _get_connection_id_by_table_name(self, table_name):
        """
        Given a table name, returns the corresponding airbyte connection
        """
        for conn in self.airbyte_api.airbyte_connections_list:
            for stream in conn["syncCatalog"]["streams"]:
                if stream["stream"]["name"].lower() == table_name:
                    return conn["connectionId"]
        return False

    def _get_source_id_by_name(self, source_name):
        for source in self.airbyte_api.airbyte_sources_list:
            if source["name"] == source_name:
                return source["sourceId"]

    def _get_destination_id_by_name(self, destination_name):
        for destination in self.airbyte_api.airbyte_destinations_list:
            if destination["name"] == destination_name:
                return destination["destinationId"]

    def _get_connection_id_by_endpoints(self, source_id, destination_id):
        for connection in self.airbyte_api.airbyte_connections_list:
            if (
                connection["sourceId"] == source_id
                and connection["destinationId"] == destination_id
            ):
                return connection

    def _connection_already_updated(self, extracted_connection, current_connection):
        extracted_copy = copy(extracted_connection)
        current_copy = copy(current_connection)
        extracted_copy.pop("sourceName")
        extracted_copy.pop("destinationName")
        current_copy.pop("connectionId")
        current_copy.pop("sourceId")
        current_copy.pop("destinationId")
        current_copy.pop("breakingChange")
        return extracted_copy == current_copy

    def _create_or_update_connection(self, connection_json: dict):
        """
        Identify source_id and destination_id by their names
        Update or create connection
        """
        source_name = connection_json["sourceName"]
        destination_name = connection_json["destinationName"]
        connection_json.pop("sourceCatalogId", "")
        source_id = self._get_source_id_by_name(source_name)
        destination_id = self._get_destination_id_by_name(destination_name)
        if not source_id or not destination_id:
            console.print(
                f"No existent source-destination pair found for {connection_json['name']}"
            )
            return
        connection = self._get_connection_id_by_endpoints(source_id, destination_id)
        if connection:
            # Connection update
            if self._connection_already_updated(connection_json, connection):
                console.print(
                    f"Connection [green]{connection['name']}[/green] already up to date. Skipping"
                )
                return
            else:
                connection_id = connection["connectionId"]
                self._delete_connection(connection_id)
                conn_name = self._create_connection(connection_json, source_id, destination_id)
                self._add_update_result("connections", conn_name)
        else:
            # Connection creation
            conn_name = self._create_connection(connection_json, source_id, destination_id)
            self.loading_results["connections"]["created"].append(conn_name)

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
                f"[red]WARNING:[/red] Current Airbyte [b]{object_definition['name']}[/b] "
                f"{object_type} connector version [b]({object_definition['dockerImageTag']})"
                f"[/b] doesn't match exported [b]{exported_json_data['name']}[/b] "
                f"version ({exported_json_data['connectorVersion']}) being loaded"
            )

    def get_config_value(self, key):
        return self.coves_config.integrated["load"]["airbyte"][key]
