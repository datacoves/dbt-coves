from typing import Dict
from requests.exceptions import RequestException
import requests, json


class AirbyteApiCallerException(Exception):
    pass


class AirbyteApiCaller:
    def api_call(self, endpoint: str, body: Dict[str, str] = None):

        """
        Generic `api caller` for contacting Airbyte
        """
        try:
            response = requests.post(endpoint, json=body)
            if response.status_code >= 200 and response.status_code < 300:
                return json.loads(response.text) if response.text else None
            else:
                raise RequestException(
                    f"Unexpected status code from airbyte in endpoint {endpoint}: {response.status_code}"
                )
        except RequestException as e:
            raise AirbyteApiCallerException(
                f"Airbyte API error in endpoint {endpoint}: " + str(e)
            )

    def __init__(self, api_host, api_port):
        airbyte_host = api_host
        airbyte_port = api_port
        airbyte_api_root = "api/v1/"
        airbyte_api_base_endpoint = f"{airbyte_host}:{airbyte_port}/{airbyte_api_root}"

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

        self.airbyte_endpoint_list_destination_definitions = (
            airbyte_api_base_endpoint + "destination_definitions/list"
        )

        self.airbyte_endpoint_list_source_definitions = (
            airbyte_api_base_endpoint + "source_definitions/list"
        )

        self.airbyte_endpoint_get_source_definition = (
            airbyte_api_base_endpoint + "source_definition_specifications/get"
        )

        self.airbyte_endpoint_get_destination_definition = (
            airbyte_api_base_endpoint + "destination_definition_specifications/get"
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
        except AirbyteApiCallerException as e:
            raise AirbyteApiCallerException(
                f"Couldn't retrieve Airbyte connections, sources and destinations {e}"
            )

    def load_definitions(self):
        self.destination_definitions = self.api_call(
            self.airbyte_endpoint_list_destination_definitions,
            self.standard_request_body,
        )["destinationDefinitions"]

        self.source_definitions = self.api_call(
            self.airbyte_endpoint_list_source_definitions, self.standard_request_body
        )["sourceDefinitions"]
