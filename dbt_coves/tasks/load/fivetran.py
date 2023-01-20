from pathlib import Path

import questionary
from rich.console import Console

from dbt_coves.utils.api_caller import FivetranApiCaller
from dbt_coves.utils.yaml import open_yaml

from .base import BaseLoadTask

console = Console()

FIVETRAN_API_BASE_URL = "https://api.fivetran.com/v1"
API_ENDPOINTS = {
    "GROUP_DETAILS": FIVETRAN_API_BASE_URL + "/groups/{group}",
    "GROUP_CREATE": FIVETRAN_API_BASE_URL + "/groups/",
    "DESTINATION_LIST": FIVETRAN_API_BASE_URL + "/groups",
    "DESTINATION_DETAILS": FIVETRAN_API_BASE_URL + "/destinations/{destination}",
    "CONNECTOR_DESTINATION_LIST": FIVETRAN_API_BASE_URL + "/groups/{destination}/connectors",
    "CONNECTOR_DETAILS": FIVETRAN_API_BASE_URL + "/connectors/{connector}",
    "CONNECTOR_SCHEMAS": FIVETRAN_API_BASE_URL + "/connectors/{connector}/schemas",
}
FIVETRAN_SECRET_MASKING = "******"


class FivetranLoaderException(Exception):
    pass


class LoadFivetranTask(BaseLoadTask):
    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        subparser = sub_parsers.add_parser(
            "fivetran",
            parents=[base_subparser],
            help="""Load Fivetran destinations and connectors from JSON files,
            along with their secrets (if required)""",
        )
        subparser.add_argument(
            "--path",
            type=str,
            help="""Path where json files will be loaded from,
            i.e. '/var/data/fivetran_extract/'""",
        )
        subparser.add_argument(
            "--api-key",
            type=str,
            help="Fivetran's API Key's secret file path",
        )
        subparser.add_argument(
            "--api-secret",
            type=str,
            help="Fivetran's API Secret's secret file path",
        )
        subparser.add_argument(
            "--secrets-path",
            type=str,
            help="Secret files location for Fivetran configuration, i.e. './secrets'",
        )
        subparser.add_argument(
            "--credentials", type=str, help="Path to Fivetran credentials YAML file"
        )
        subparser.set_defaults(cls=cls, which="fivetran")
        return subparser

    def get_config_value(self, key):
        return self.coves_config.integrated["load"]["fivetran"][key]

    def run(self) -> int:
        self.load_results = {
            "groups": {"created": set()},
            "destinations": {"created": set(), "updated": set()},
            "connectors": {"created": set(), "updated": set()},
        }

        extract_destination = self.get_config_value("path")
        self.api_key = self.get_config_value("api_key")
        self.api_secret = self.get_config_value("api_secret")
        self.secrets_manager = self.get_config_value("secrets_manager")
        self.secrets_url = self.get_config_value("secrets_url")
        self.secrets_token = self.get_config_value("secrets_token")
        secrets_path = self.get_config_value("secrets_path")
        api_credentials_path = self.get_config_value("credentials")

        if api_credentials_path and (self.api_key or self.api_secret):
            raise FivetranLoaderException(
                "Flags 'credentials' and 'api key/secret' ones are mutually exclusive."
            )

        if not extract_destination or not (
            (self.api_key and self.api_secret) or api_credentials_path
        ):
            raise FivetranLoaderException(
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

        # Load all previously extracted Connectors and Destinations
        self.extracted_destinations = self.retrieve_all_jsons_from_path(
            str(self.extract_destination.absolute())
        )
        if not self.extracted_destinations:
            raise FivetranLoaderException(
                f"No Fivetran extracted data found on {self.extract_destination.absolute()}"
            )

        self.loaded_secrets = []
        if secrets_path:
            self.secrets_path = Path(secrets_path)
            self.loaded_secrets = self.retrieve_all_jsons_from_path(
                str(self.secrets_path.absolute())
            )

        for fivetran_destination in self.extracted_destinations:
            if self.loaded_secrets:
                self._load_fivetran_object_secrets(fivetran_destination)
            for destination_data in fivetran_destination.values():
                destination_details = destination_data["details"]

                exported_group_id = destination_details["group_id"]
                exported_service_type = destination_details["service"]

                target_group_id = self._get_or_create_fivetran_group_id(
                    exported_group_id, exported_service_type
                )

                if target_group_id != exported_group_id:
                    self._update_group_id(fivetran_destination, target_group_id)

                self._update_or_create_fivetran_destination(fivetran_destination, target_group_id)

        if (
            len(self.load_results["destinations"]["updated"]) > 0
            or len(self.load_results["destinations"]["created"]) > 0
        ):
            for obj_type, result_dict in self.load_results.items():
                for activity, result in result_dict.items():
                    if len(result) > 0:
                        console.print(
                            f"{obj_type.capitalize()} {activity}:"
                            f"[green]{', '.join(result)}[/green]"
                        )
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

    def _update_fivetran_destination(self, destination_details):
        destination_id = destination_details["id"]
        destination_name = self.fivetran_api.fivetran_groups[destination_details["group_id"]][
            "name"
        ]
        updated_destination = self.fivetran_api.update_destination(
            destination_id, destination_details
        )
        self.load_results["destinations"]["updated"].add(destination_name)
        return updated_destination

    def _create_fivetran_destination(self, destination_details):
        created_destination = {}
        del destination_details["id"]
        created_destination = self.fivetran_api.create_destination(destination_details)
        destination_name = self.fivetran_api.fivetran_groups[created_destination["group_id"]][
            "name"
        ]
        self.load_results["destinations"]["created"].add(destination_name)
        return created_destination

    def _update_fivetran_connector(self, connector_details):
        connector_id = connector_details["id"]
        updated_connector = self.fivetran_api.update_connector(connector_id, connector_details)
        connector_schema = updated_connector["schema"]
        self.load_results["connectors"]["updated"].add(connector_schema)
        return updated_connector

    def _create_fivetran_connector(self, connector_details):
        del connector_details["id"]
        console.print("Creating Fivetran connector")
        created_connector = self.fivetran_api.create_connector(connector_details)
        connector_schema = created_connector["schema"]
        self.load_results["connectors"]["created"].add(connector_schema)
        return created_connector

    def _update_target_connector_schema_config(self, connector, schemas):
        connector_id = connector["id"]
        connector_schemas = self.fivetran_api._get_connector_schemas(connector_id)
        if connector_schemas:
            self.fivetran_api.update_connector_schema_config(connector_id, schemas)

    def _get_existent_destination(self, target_group_id):
        for dest_data in self.fivetran_api.fivetran_data.values():
            existent_dest_details = dest_data["details"]
            if existent_dest_details["group_id"] == target_group_id:
                return dest_data
        return {}

    def _load_fivetran_object_secrets(self, obj):
        """
        Identify secret files' key:values and replace Fivetran object ones
        """
        for secret in self.loaded_secrets:
            for fivetran_obj_name, secret_data in secret.items():
                for destination_data in obj.values():
                    object_details = destination_data["details"]
                    if fivetran_obj_name == object_details["id"]:
                        for k, v in secret_data.items():
                            self._replace_dict_key(object_details, k, v)

    def _update_or_create_fivetran_destination(self, exported_destination, target_group_id):
        """
        Given exported destination data
        - update if ID exists
        - create a new one if doesn't
        """
        group_name = self.fivetran_api.fivetran_groups[target_group_id]["name"]
        destination_data = {}
        # get details
        for destination_data in exported_destination.values():
            exported_dest_details = destination_data["details"]
            exported_dest_details["run_setup_tests"] = False
            exported_dest_connectors = destination_data.get("connectors", {})

        # if ID in fivetran_data
        current_destination = self._get_existent_destination(target_group_id)
        if current_destination:
            # update
            self._update_fivetran_destination(exported_dest_details)
        else:
            # create
            self._create_fivetran_destination(exported_dest_details)

        for exported_connector in exported_dest_connectors.values():
            exported_connector_details = exported_connector["details"]
            exported_connector_details["run_setup_tests"] = False
            self._fill_required_config_fields(exported_connector_details, group_name)
            if current_destination and self._exported_connector_exists_in_destination(
                exported_connector_details, current_destination
            ):
                target_connector = self._update_fivetran_connector(exported_connector_details)
            else:
                target_connector = self._create_fivetran_connector(exported_connector_details)

            exported_schemas = exported_connector.get("schemas", {})
            if exported_schemas:
                self._update_target_connector_schema_config(target_connector, exported_schemas)

    def _fill_required_config_fields(self, connector_details, group_name):
        service_type = connector_details["service"]
        required_config_fields = self.fivetran_api.get_service_required_fields(service_type)
        for field in required_config_fields:
            if not connector_details["config"].get(field):
                connector_details["config"][field] = questionary.text(
                    f"Enter new {field} for exported {service_type} "
                    f"Connector {connector_details['schema']} in Destination {group_name}:"
                ).ask()

    def _exported_connector_exists_in_destination(self, exported_connector, existent_destination):
        exported_connector_name = exported_connector["schema"]
        for existent_connector in existent_destination.get("connectors", {}).values():
            if existent_connector["details"]["schema"] == exported_connector_name:
                exported_connector["id"] = existent_connector["details"]["id"]
                return True
        return False

    def _replace_dict_key(self, obj, searched_key, replace_value):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if k == searched_key:
                    obj[k] = replace_value
                if isinstance(v, dict):
                    self._replace_dict_key(v, searched_key, replace_value)
        elif isinstance(obj, list):
            for item in obj:
                self._replace_dict_key(item, searched_key, replace_value)

    def _update_group_id(self, obj, new_group_id):
        self._replace_dict_key(obj, "group_id", new_group_id)
        for dest_data in obj.values():
            dest_details = dest_data["details"]
            self._replace_dict_key(dest_details, "id", new_group_id)

    def _get_or_create_fivetran_group_id(self, group_id, service_type):
        group_candidates_ids = set()

        for existent_group_id, group_data in self.fivetran_api.fivetran_groups.items():
            if group_id == existent_group_id:
                return existent_group_id
            if service_type == group_data["service"]:
                group_candidates_ids.add(existent_group_id)

        console.print(f"Extracted Fivetran group with ID [red]{group_id}[/red] not found.")

        # Path 2: get destination with the same Service type
        if group_candidates_ids:
            selected_group = questionary.select(
                "Would you like to update one of the following Groups/Destinations?:",
                choices=[
                    group_data["name"]
                    for group_id, group_data in self.fivetran_api.fivetran_groups.items()
                    if group_id in group_candidates_ids
                ]
                + ["(Create Group)"],
            ).ask()
            if selected_group == "(Create Group)":
                return self._create_fivetran_group(service_type)
            else:
                for group_id, group_data in self.fivetran_api.fivetran_groups.items():
                    if selected_group == group_data["name"]:
                        return group_id

        # Path 3: no groups found with the same service type -> Create
        else:
            return self._create_fivetran_group(service_type)

    def _create_fivetran_group(self, service):
        group_name = questionary.text("Enter a name for your new Fivetran Group:").ask()
        created_group_id = self.fivetran_api.create_group(group_name, service)
        group_name = self.fivetran_api.fivetran_groups[created_group_id]["name"]
        self.load_results["groups"]["created"].add(group_name)
        return created_group_id
