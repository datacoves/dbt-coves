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
            help="Airbyte's API host, i.e. 'http://airbyte-server'",
        )
        subparser.add_argument(
            "--port",
            type=str,
            help="Airbyte's API port, i.e. '8006'",
        )
        subparser.add_argument(
            "--api-key",
            type=str,
            help="Airbyte's API key for Bearer token auth (optional for open OSS instances)",
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
        subparser.add_argument(
            "--secrets-url", type=str, help="Secret credentials provider url"
        )
        subparser.add_argument(
            "--secrets-token", type=str, help="Secret credentials provider token"
        )
        subparser.add_argument(
            "--secrets-environment", type=str, help="Secret credentials project"
        )
        subparser.add_argument(
            "--secrets-tags", type=str, help="Secret credentials tags"
        )
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
        self.airbyte_api_key = self.get_config_value("api_key")
        self.secrets_path = self.get_config_value("secrets_path")
        self.secrets_manager = self.get_config_value("secrets_manager")

        if not (self.airbyte_host and self.load_destination):
            raise AirbyteLoaderException(
                "'path' and 'host' are required parameters in order to load Airbyte. "
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

        self.airbyte_api = AirbyteApiCaller(
            self.airbyte_host,
            api_port=self.airbyte_port or None,
            api_key=self.airbyte_api_key or None,
        )

        console.print(
            f"Loading DBT Sources into Airbyte from {os.path.abspath(path)}\n"
        )

        extracted_sources = self.retrieve_all_jsons_from_path(
            self.sources_load_destination
        )
        extracted_destinations = self.retrieve_all_jsons_from_path(
            self.destinations_load_destination
        )
        extracted_connections = self.retrieve_all_jsons_from_path(
            self.connections_load_destination
        )
        for source in extracted_sources:
            self._create_or_update_source(source)
        for destination in extracted_destinations:
            self._create_or_update_destination(destination)
        for connection in extracted_connections:
            self._create_or_update_connection(connection)

        self._print_load_results()
        return 0

    def _print_load_results(self):
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
        configuration,
        wildcard_pattern,
        secret_data,
        secret_target_name,
    ):
        for k, v in configuration.items():
            if wildcard_pattern == str(v):
                configuration[k] = self._get_secret_value_for_field(
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
        Resolve masked secret fields in a source/destination's configuration.
        """
        try:
            configuration = exported_json_data.get("configuration", {})
            secret_target_name = slugify(exported_json_data["name"].lower())
            wildcard_pattern = "**********"

            hidden_fields = list()
            for config_field, value in configuration.items():
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

                self._update_connection_config_secret_fields(
                    configuration,
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

    def _update_source_or_destination(self, exported_data, object_type, object_id):
        update = True
        conn_status = self._test_existing_object(object_id, object_type)
        if conn_status and conn_status.get("status", "").lower() != "succeeded":
            console.print(
                f"Connection test for {exported_data['name']} failed:\n "
                f"{conn_status.get('message', '')}"
            )
            update = questionary.confirm(
                "Would you like to continue updating it?"
            ).ask()
        if update:
            update_body = {
                "name": exported_data["name"],
                "configuration": exported_data.get("configuration", {}),
            }
            if object_type == "sources":
                response = self.airbyte_api.update_source(object_id, update_body)
            else:
                response = self.airbyte_api.update_destination(object_id, update_body)
            self._add_update_result(object_type, exported_data["name"])
            return response.get("sourceId", response.get("destinationId"))

    def _create_source_or_destination(self, exported_data, object_type):
        if object_type == "sources":
            response = self.airbyte_api.create_source(exported_data)
        else:
            response = self.airbyte_api.create_destination(exported_data)

        object_id = response.get("sourceId", response.get("destinationId", ""))
        object_name = response.get("name", "")

        conn_status = self._test_existing_object(object_id, object_type)
        create = True
        if conn_status and conn_status.get("status", "").lower() != "succeeded":
            console.print(
                f"Connection test for {exported_data['name']} failed:\n "
                f"{conn_status.get('message', '')}"
            )
            create = questionary.confirm(
                "Would you like to continue creating it?"
            ).ask()

        if create:
            if object_type == "sources":
                self.airbyte_api.sources_list.append(response)
            else:
                self.airbyte_api.destinations_list.append(response)
            self.loading_results[object_type]["created"].append(object_name)
            return object_id
        else:
            if object_type == "sources":
                self.airbyte_api.delete_source(object_id)
            else:
                self.airbyte_api.delete_destination(object_id)

    def _test_existing_object(self, object_id, object_type, timeout=30):
        """Test connection for an existing source or destination by ID."""
        console.print(f"Testing [yellow]{object_type[:-1]}[/yellow] connection")
        try:
            if object_type == "sources":
                return self.airbyte_api.check_source(object_id, timeout=timeout)
            else:
                return self.airbyte_api.check_destination(object_id, timeout=timeout)
        except AirbyteApiCallerException as e:
            console.print(f"[yellow]Warning:[/yellow] Connection test failed: {e}")
            return None

    def _create_source(self, exported_data):
        exported_data.pop("connectorVersion", None)
        exported_data = self._get_secrets(exported_data, "sources")
        exported_data["workspaceId"] = self.airbyte_api.workspace_id
        # sourceType is already in the exported JSON from extract — no definition ID lookup needed
        self._create_source_or_destination(exported_data, "sources")

    def _update_source(self, exported_data, source_id):
        exported_data.pop("connectorVersion", None)
        exported_data = self._get_secrets(exported_data, "sources")
        self._update_source_or_destination(exported_data, "sources", source_id)

    def _sources_are_equivalent(self, exported_source, current_source):
        current_copy = copy(current_source)
        exported_copy = copy(exported_source)
        exported_copy.pop("connectorVersion", None)
        # Strip server-managed fields from the live source before comparing
        for field in ("sourceId", "workspaceId", "createdAt", "updatedAt", "icon"):
            current_copy.pop(field, None)
        return current_copy == exported_copy

    def _create_or_update_source(self, exported_json_data):
        """
        Decide whether to create or update a source based on name matching.
        """
        self._connector_versions_mismatch(exported_json_data, "source")

        for src in self.airbyte_api.sources_list:
            if exported_json_data["name"] == src["name"]:
                if self._sources_are_equivalent(exported_json_data, src):
                    console.print(
                        f"Source [green]{src['name']}[/green] already up to date. Skipping"
                    )
                    return
                else:
                    return self._update_source(exported_json_data, src["sourceId"])

        return self._create_source(exported_json_data)

    def _get_destination_definition_by_type(self, destination_type):
        """Get destination definition by destinationType string."""
        for definition in self.airbyte_api.destination_definitions:
            repo = definition.get("dockerRepository", "")
            repo_suffix = repo.split("/")[-1]
            if repo_suffix == f"destination-{destination_type}":
                return definition
            if definition.get("connectorType") == destination_type:
                return definition
        raise AirbyteLoaderException(
            f"There is no destination definition for type '{destination_type}'. "
            "Please review Airbyte's configuration"
        )

    def _get_source_definition_by_type(self, source_type):
        """Get source definition by sourceType string."""
        for definition in self.airbyte_api.source_definitions:
            repo = definition.get("dockerRepository", "")
            repo_suffix = repo.split("/")[-1]
            if repo_suffix == f"source-{source_type}":
                return definition
            if definition.get("connectorType") == source_type:
                return definition
        raise AirbyteLoaderException(
            f"There is no source definition for type '{source_type}'. "
            "Please review Airbyte's configuration"
        )

    def _create_destination(self, exported_data):
        exported_data.pop("connectorVersion", None)
        exported_data = self._get_secrets(exported_data, "destinations")
        exported_data["workspaceId"] = self.airbyte_api.workspace_id
        self._create_source_or_destination(exported_data, "destinations")

    def _update_destination(self, exported_data, destination_id):
        exported_data.pop("connectorVersion", None)
        exported_data = self._get_secrets(exported_data, "destinations")
        self._update_source_or_destination(
            exported_data, "destinations", destination_id
        )

    def _destinations_are_equivalent(self, exported_destination, current_destination):
        current_copy = copy(current_destination)
        exported_copy = copy(exported_destination)
        exported_copy.pop("connectorVersion", None)
        for field in ("destinationId", "workspaceId", "createdAt", "updatedAt", "icon"):
            current_copy.pop(field, None)
        return current_copy == exported_copy

    def _create_or_update_destination(self, exported_json_data):
        """
        Decide whether to create or update a destination based on name matching.
        """
        self._connector_versions_mismatch(exported_json_data, "destination")

        for destination in self.airbyte_api.destinations_list:
            if exported_json_data["name"] == destination["name"]:
                if self._destinations_are_equivalent(exported_json_data, destination):
                    console.print(
                        f"Destination [green]{destination['name']}[/green] "
                        f"already up to date. Skipping"
                    )
                    return
                else:
                    return self._update_destination(
                        exported_json_data, destination["destinationId"]
                    )

        return self._create_destination(exported_json_data)

    SYNC_MODE_MAP = {
        ("full_refresh", "overwrite"): "full_refresh_overwrite",
        ("full_refresh", "append"): "full_refresh_append",
        ("incremental", "append"): "incremental_append",
        ("incremental", "append_dedup"): "incremental_deduped_history",
    }

    NAMESPACE_DEFINITION_MAP = {
        "customformat": "custom_format",
        "nsformat": "custom_format",
    }

    def _normalize_connection_fields(self, connection):
        """Normalize field values that changed between the old internal API and the public API."""
        ns_def = connection.get("namespaceDefinition")
        if ns_def in self.NAMESPACE_DEFINITION_MAP:
            connection["namespaceDefinition"] = self.NAMESPACE_DEFINITION_MAP[ns_def]
        if connection.get("prefix") == "":
            connection.pop("prefix")
        return connection

    def _normalize_connection_catalog(self, connection):
        """
        Convert syncCatalog (old internal API) to configurations (public API).
        Skips deselected streams. Maps combined syncMode+destinationSyncMode to
        the single syncMode string the public API expects.
        """
        sync_catalog = connection.pop("syncCatalog", None)
        if sync_catalog is None or "configurations" in connection:
            return connection

        streams = []
        for entry in sync_catalog.get("streams", []):
            stream = entry.get("stream", {})
            config = entry.get("config", {})
            if not config.get("selected", True):
                continue
            sync_mode = config.get("syncMode", "full_refresh")
            dest_sync_mode = config.get("destinationSyncMode", "overwrite")
            new_mode = self.SYNC_MODE_MAP.get(
                (sync_mode, dest_sync_mode), f"{sync_mode}_{dest_sync_mode}"
            )
            stream_config = {"name": stream.get("name"), "syncMode": new_mode}
            if stream.get("namespace"):
                stream_config["streamNamespace"] = stream["namespace"]
            if config.get("cursorField"):
                stream_config["cursorField"] = config["cursorField"]
            if config.get("primaryKey"):
                stream_config["primaryKey"] = config["primaryKey"]
            streams.append(stream_config)

        connection["configurations"] = {"streams": streams}
        return connection

    def _normalize_connection_schedule(self, connection):
        """
        Convert flat scheduleType/scheduleData (old internal API) to the
        nested schedule object the public API expects.
        """
        schedule_type = connection.pop("scheduleType", None)
        schedule_data = connection.pop("scheduleData", None)
        if schedule_type and "schedule" not in connection:
            schedule = {"scheduleType": schedule_type}
            if schedule_data:
                schedule["cronExpression"] = schedule_data.get("cron", {}).get(
                    "cronExpression"
                ) or schedule_data.get("basicSchedule", {}).get("cronExpression")
            connection["schedule"] = {
                k: v for k, v in schedule.items() if v is not None
            }
        return connection

    def _create_connection(self, exported_json_data, source_id, destination_id):
        exported_json_data["sourceId"] = source_id
        exported_json_data["destinationId"] = destination_id
        connection_name = f"{exported_json_data['sourceName']}-{exported_json_data['destinationName']}"
        exported_json_data.pop("sourceName", None)
        exported_json_data.pop("destinationName", None)
        exported_json_data.pop("operationIds", None)
        exported_json_data = self._normalize_connection_fields(exported_json_data)
        exported_json_data = self._normalize_connection_schedule(exported_json_data)
        exported_json_data = self._normalize_connection_catalog(exported_json_data)

        try:
            response = self.airbyte_api.create_connection(exported_json_data)
            if response:
                self.airbyte_api.connections_list.append(response)
                return connection_name
        except AirbyteApiCallerException as ex:
            raise AirbyteApiCallerException(
                f"Could not create Airbyte connection: {ex}"
            )

    def _delete_connection(self, connection_id):
        try:
            self.airbyte_api.delete_connection(connection_id)
        except AirbyteApiCallerException:
            raise AirbyteLoaderException(
                "Could not delete Airbyte connection for re-creation"
            )

    def _get_source_id_by_name(self, source_name):
        for source in self.airbyte_api.sources_list:
            if source["name"] == source_name:
                return source["sourceId"]

    def _get_destination_id_by_name(self, destination_name):
        for destination in self.airbyte_api.destinations_list:
            if destination["name"] == destination_name:
                return destination["destinationId"]

    def _get_connection_by_endpoints(self, source_id, destination_id):
        for connection in self.airbyte_api.connections_list:
            if (
                connection["sourceId"] == source_id
                and connection["destinationId"] == destination_id
            ):
                return connection

    def _connection_already_updated(self, extracted_connection, current_connection):
        extracted_copy = copy(extracted_connection)
        current_copy = copy(current_connection)
        extracted_copy.pop("sourceName", None)
        extracted_copy.pop("destinationName", None)
        for field in (
            "connectionId",
            "sourceId",
            "destinationId",
            "breakingChange",
            "createdAt",
            "updatedAt",
        ):
            current_copy.pop(field, None)
        return extracted_copy == current_copy

    def _create_or_update_connection(self, connection_json: dict):
        """
        Identify source_id and destination_id by their names, then update or create.
        """
        source_name = connection_json["sourceName"]
        destination_name = connection_json["destinationName"]
        connection_json.pop("sourceCatalogId", None)
        source_id = self._get_source_id_by_name(source_name)
        destination_id = self._get_destination_id_by_name(destination_name)
        if not source_id or not destination_id:
            console.print(
                f"No existent source-destination pair found for connection "
                f"({source_name} → {destination_name})"
            )
            return
        connection = self._get_connection_by_endpoints(source_id, destination_id)
        if connection:
            if self._connection_already_updated(connection_json, connection):
                console.print(
                    f"Connection [green]{connection['name']}[/green] already up to date. Skipping"
                )
                return
            else:
                connection_id = connection["connectionId"]
                self._delete_connection(connection_id)
                conn_name = self._create_connection(
                    connection_json, source_id, destination_id
                )
                self._add_update_result("connections", conn_name)
        else:
            conn_name = self._create_connection(
                connection_json, source_id, destination_id
            )
            self.loading_results["connections"]["created"].append(conn_name)

    def _add_update_result(self, obj_type, obj_name):
        if (obj_name not in self.loading_results[obj_type]["updated"]) and (
            obj_name not in self.loading_results[obj_type]["created"]
        ):
            self.loading_results[obj_type]["updated"].append(obj_name)

    def _connector_versions_mismatch(self, exported_json_data, object_type):
        try:
            if object_type == "source":
                source_type = exported_json_data.get("sourceType", "")
                object_definition = self._get_source_definition_by_type(source_type)
            else:
                destination_type = exported_json_data.get("destinationType", "")
                object_definition = self._get_destination_definition_by_type(
                    destination_type
                )
        except AirbyteLoaderException:
            return

        current_version = object_definition.get("dockerImageTag", "unknown")
        saved_version = exported_json_data.get("connectorVersion", "unknown")
        if saved_version != "unknown" and saved_version != current_version:
            connector_name = object_definition.get("name", object_type)
            obj_name = exported_json_data.get("name", "")
            console.print(
                f"[red]WARNING:[/red] Current Airbyte [b]{connector_name}[/b] "
                f"{object_type} connector version [b]({current_version})[/b] "
                f"doesn't match exported [b]{obj_name}[/b] "
                f"version ({saved_version}) being loaded"
            )

    def get_config_value(self, key):
        return self.coves_config.integrated["load"]["airbyte"][key]
