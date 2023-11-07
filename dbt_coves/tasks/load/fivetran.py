from pathlib import Path

import questionary
from rich.console import Console

from dbt_coves.utils.api_caller import FivetranApiCaller
from dbt_coves.utils.secrets import load_secret_manager_data
from dbt_coves.utils.tracking import trackable
from dbt_coves.utils.yaml import open_yaml

from .base import BaseLoadTask

console = Console()


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
            "--credentials", type=str, help="Path to Fivetran credentials YAML file"
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
            "--secrets-manager", type=str, help="Secret credentials provider, i.e. 'datacoves'"
        )
        subparser.add_argument("--secrets-url", type=str, help="Secret credentials provider url")
        subparser.add_argument(
            "--secrets-token", type=str, help="Secret credentials provider token"
        )
        subparser.add_argument("--secrets-project", type=str, help="Secret credentials project")
        subparser.add_argument("--secrets-tags", type=str, help="Secret credentials tags")
        subparser.add_argument("--secrets-key", type=str, help="Secret credentials key")
        subparser.set_defaults(cls=cls, which="fivetran")
        return subparser

    def get_config_value(self, key):
        return self.coves_config.integrated["load"]["fivetran"][key]

    def _print_load_results(self):
        for obj_type, result_dict in self.load_results.items():
            for activity, result in result_dict.items():
                if len(result) > 0:
                    console.print(
                        f"{obj_type.capitalize()} {activity}: "
                        f"[green]{', '.join(result)}[/green]"
                    )

    @trackable
    def run(self) -> int:
        self.load_results = {
            "groups": {"created": set()},
            "destinations": {"created": set(), "updated": set()},
            "connectors": {"created": set(), "updated": set()},
            "schemas": {"updated": set()},
            "tables": {"updated": set()},
        }

        extract_destination = self.get_config_value("path")
        self.api_key = self.get_config_value("api_key")
        self.api_secret = self.get_config_value("api_secret")
        self.secrets_manager = self.get_config_value("secrets_manager")
        api_credentials_path = self.get_config_value("credentials")
        secrets_path = self.get_config_value("secrets_path")

        if secrets_path and self.secrets_manager:
            raise FivetranLoaderException(
                "Can't use 'secrets_path' and 'secrets_manager' simultaneously."
            )
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

        self.local_secrets = []
        self.secret_manager_data = {}

        if secrets_path:
            self.secrets_path = Path(secrets_path)
            self.local_secrets = self.retrieve_all_jsons_from_path(
                str(self.secrets_path.absolute())
            )

        if self.secrets_manager:
            self.secret_manager_data = load_secret_manager_data(self)

        for fivetran_destination in self.extracted_destinations:
            if self.local_secrets:
                self._load_fivetran_local_secrets(fivetran_destination)
            elif self.secret_manager_data and self.secrets_manager.lower() == "datacoves":
                self._load_fivetran_datacoves_secrets(fivetran_destination)

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
        self._print_load_results()

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

    def _fivetran_destination_updated(self, destination_details):
        current_destination = self.fivetran_api._get_destination_details(destination_details["id"])
        return destination_details == current_destination

    def _update_fivetran_destination(self, destination_details):
        destination_id = destination_details["id"]
        destination_name = self.fivetran_api.fivetran_groups[destination_details["group_id"]][
            "name"
        ]
        if self._fivetran_destination_updated(destination_details):
            console.print(
                f"Destination [green]{destination_details['id']}[/green] already up to date. Skipping"
            )
        else:
            destination_details["run_setup_tests"] = False
            self.fivetran_api.update_destination(destination_id, destination_details)
            self.load_results["destinations"]["updated"].add(destination_name)

    def _create_fivetran_destination(self, destination_details):
        created_destination = {}
        del destination_details["id"]
        destination_details["run_setup_tests"] = False
        created_destination = self.fivetran_api.create_destination(destination_details)
        destination_name = self.fivetran_api.fivetran_groups[created_destination["group_id"]][
            "name"
        ]
        self.load_results["destinations"]["created"].add(destination_name)
        return created_destination

    def _fivetran_connector_updated(self, connector_details):
        current_connector = self.fivetran_api._get_connector_details(connector_details["id"])
        return connector_details == current_connector

    def _update_fivetran_connector(self, connector_details, group_name):
        connector_id = connector_details["id"]
        if self._fivetran_connector_updated(connector_details):
            console.print(f"Connector [green]{connector_id}[/green] already up to date. Skipping")
            return connector_details
        connector_details["run_setup_tests"] = False
        self._fill_required_config_fields(connector_details, group_name)
        updated_connector = self.fivetran_api.update_connector(connector_id, connector_details)
        connector_schema = updated_connector["schema"]
        self.load_results["connectors"]["updated"].add(connector_schema)
        return updated_connector

    def _create_fivetran_connector(self, connector_details):
        del connector_details["id"]
        console.print("Creating Fivetran connector")
        connector_details["run_setup_tests"] = False
        created_connector = self.fivetran_api.create_connector(connector_details)
        connector_schema = created_connector["schema"]
        self.load_results["connectors"]["created"].add(connector_schema)
        return created_connector

    def _update_target_connector_schema_config(self, connector, extracted_schemas):
        connector_id = connector["id"]
        connector_schemas = self.fivetran_api._get_connector_schemas(connector_id)
        if connector_schemas:
            self.fivetran_api.update_connector_schema_config(connector_id, extracted_schemas)
            for extracted_schema in extracted_schemas.values():
                schema_name_in_destination = extracted_schema["name_in_destination"]
                self.load_results["schemas"]["updated"].add(schema_name_in_destination)
                for table_config in extracted_schema.get("tables", {}).values():
                    table_name_in_destination = table_config["name_in_destination"]
                    self.fivetran_api.update_connector_table_config(
                        connector_id,
                        schema_name_in_destination,
                        table_name_in_destination,
                        table_config,
                    )
                    self.load_results["tables"]["updated"].add(table_name_in_destination)

    def _get_existent_destination(self, target_group_id):
        for dest_data in self.fivetran_api.fivetran_data.values():
            existent_dest_details = dest_data["details"]
            if existent_dest_details["group_id"] == target_group_id:
                return dest_data
        return {}

    def _load_fivetran_local_secrets(self, fivetran_object):
        """
        Identify secret files' key:values and replace Fivetran object ones
        """
        for secret in self.local_secrets:
            for fivetran_obj_name, secret_data in secret.items():
                for destination_data in fivetran_object.values():
                    object_details = destination_data["details"]
                    if fivetran_obj_name == object_details["id"]:
                        for k, v in secret_data.items():
                            self._replace_dict_key(object_details, k, v)

    def _load_fivetran_datacoves_secrets(self, fivetran_object):
        for secret in self.secret_manager_data:
            for destination_data in fivetran_object.values():
                object_details = destination_data["details"]
                if object_details["id"] == secret.get("slug", ""):
                    for k, v in secret.get("value", {}).items():
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
            if current_destination and self._exported_connector_exists_in_destination(
                exported_connector_details, current_destination
            ):
                target_connector = self._update_fivetran_connector(
                    exported_connector_details, group_name
                )
            else:
                self._fill_required_config_fields(exported_connector_details, group_name)
                target_connector = self._create_fivetran_connector(exported_connector_details)

            exported_schemas = exported_connector.get("schemas", {})
            if exported_schemas:
                self._update_target_connector_schema_config(target_connector, exported_schemas)

    def _fill_required_config_fields(self, connector_details, group_name):
        """
        Get required fields based on Metadata call
        - Set Connector required fields in Config
        - If a Connector has Reports,
        make sure Config and Reports don't have the same fields
        """
        service_type = connector_details["service"]
        connector_config = connector_details["config"]
        required_config_fields = self.fivetran_api.get_service_required_fields(service_type)
        for field in required_config_fields:
            if not connector_config.get(field):
                connector_config[field] = questionary.text(
                    f"Enter new {field} for exported {service_type} "
                    f"Connector {connector_details['schema']} in Destination {group_name}:"
                ).ask()

        # Avoid field repetition in Config and Reports (PATCH/POST legacy mode workaround)
        if connector_config.get("reports"):
            for report in connector_config["reports"]:
                for report_field in list(report):
                    if report_field in connector_config:
                        del connector_config[report_field]

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

            if service_type == group_data.get("service", ""):
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
