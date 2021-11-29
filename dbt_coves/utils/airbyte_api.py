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
            airbyte_api_base_endpoint = (
                f"http://{airbyte_host}:{airbyte_port}/{airbyte_api_root}"
            )
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
