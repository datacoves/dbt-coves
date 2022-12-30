import json
from typing import Any, Dict

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException

FIVETRAN_API_BASE_URL = "https://api.fivetran.com/v1"
FIVETRAN_API_ENDPOINTS = {
    "GROUP_CREATE": FIVETRAN_API_BASE_URL + "/groups",
    "GROUP_DETAILS": FIVETRAN_API_BASE_URL + "/groups/{group}",
    "DESTINATION_LIST": FIVETRAN_API_BASE_URL + "/groups",
    "DESTINATION_DETAILS": FIVETRAN_API_BASE_URL + "/destinations/{destination}",
    "DESTINATION_CREATE": FIVETRAN_API_BASE_URL + "/destinations",
    "CONNECTOR_DESTINATION_LIST": FIVETRAN_API_BASE_URL + "/groups/{destination}/connectors",
    "CONNECTOR_CREATE": FIVETRAN_API_BASE_URL + "/connectors/",
    "CONNECTOR_DETAILS": FIVETRAN_API_BASE_URL + "/connectors/{connector}",
    "CONNECTOR_SCHEMAS": FIVETRAN_API_BASE_URL + "/connectors/{connector}/schemas",
    "SOURCE_METADATA": FIVETRAN_API_BASE_URL + "/metadata/connectors/{service}",
}


def api_call(
    method,
    endpoint: str,
    body: Dict[str, str] = None,
    headers=None,
    auth=None,
):
    """Generic `api caller`"""
    response = requests.request(method, url=endpoint, json=body, headers=headers, auth=auth)
    try:
        if response.status_code == 404:
            return {}
        else:
            response.raise_for_status()
            return json.loads(response.text)
    except RequestException:
        error_message = json.loads(response.text)["message"]
        raise FivetranApiCallerException(error_message)


class AirbyteApiCallerException(Exception):
    pass


class FivetranApiCallerException(Exception):
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
                    f"Unexpected status code from airbyte in endpoint {endpoint}:"
                    f"{response.status_code}: {json.loads(response.text)['message']}"
                )
        except RequestException as e:
            raise AirbyteApiCallerException(f"Airbyte API error in endpoint {endpoint}: " + str(e))

    def __init__(self, api_host, api_port):
        airbyte_host = api_host
        airbyte_port = api_port
        airbyte_api_root = "api/v1/"
        airbyte_api_base_endpoint = f"{airbyte_host}:{airbyte_port}/{airbyte_api_root}"

        airbyte_api_list_component = airbyte_api_base_endpoint + "{component}/list"
        self.airbyte_endpoint_list_connections = airbyte_api_list_component.format(
            component="connections"
        )
        self.airbyte_endpoint_list_sources = airbyte_api_list_component.format(component="sources")
        self.airbyte_endpoint_list_destinations = airbyte_api_list_component.format(
            component="destinations"
        )

        airbyte_endpoint_list_workspaces = airbyte_api_list_component.format(component="workspaces")

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
        self.airbyte_endpoint_delete_connection = airbyte_api_base_endpoint + "connections/delete"

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

            self.load_definitions()
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


class FivetranApiCaller:
    def __init__(self, api_key, api_secret):
        self.auth = HTTPBasicAuth(api_key, api_secret)
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "application/json;version=2",
        }
        self.fivetran_data = self._populate_fivetran_data()

    def _fivetran_api_call(self, method: str, endpoint: str, payload=None):
        """
        Common method to reach Fivetran API, extensible for future Methods and Endpoints
        """
        return api_call(
            method,
            endpoint,
            body=payload,
            headers=self.headers,
            auth=self.auth,
        )

    def _get_destination_details(self, destination_id) -> Dict[Any, Any]:
        """
        Get Group details from Fivetran API
        """
        destination_details = self._fivetran_api_call(
            "GET",
            FIVETRAN_API_ENDPOINTS["DESTINATION_DETAILS"].format(destination=destination_id),
        )
        return destination_details.get("data")

    def _get_connector_schemas(self, connector_id):
        """
        Get Connector schemas from Fivetran API
        """
        connector_schemas = self._fivetran_api_call(
            "GET",
            FIVETRAN_API_ENDPOINTS["CONNECTOR_SCHEMAS"].format(connector=connector_id),
        )
        return connector_schemas.get("data", {}).get("schemas", {})

    def _get_connector_details(self, connector_id):
        """
        Get Connector details from Fivetran API
        """
        connector_details = self._fivetran_api_call(
            "GET",
            FIVETRAN_API_ENDPOINTS["CONNECTOR_DETAILS"].format(connector=connector_id),
        )
        return connector_details.get("data", {})

    def _get_destination_connectors(self, destination_id: str) -> Dict[Any, Any]:
        """
        Get Group connectors (ids and their schemas)
        """
        destination_connectors = self._fivetran_api_call(
            "GET",
            FIVETRAN_API_ENDPOINTS["CONNECTOR_DESTINATION_LIST"].format(destination=destination_id),
        )
        connector_data = {}
        for connector in destination_connectors.get("data", {}).get("items", []):
            connector_id = connector["id"]
            connector_data[connector_id] = {}
            connector_data[connector_id]["details"] = self._get_connector_details(connector_id)
            connector_data[connector_id]["schemas"] = self._get_connector_schemas(connector_id)
        return connector_data

    def create_group(self, group_name, service) -> str:
        payload = {"name": group_name}
        created_group = self._fivetran_api_call(
            "POST", FIVETRAN_API_ENDPOINTS["GROUP_CREATE"], payload=payload
        )
        created_group_id = created_group["data"]["id"]
        created_group_name = created_group["data"]["name"]
        self.fivetran_groups[created_group_id] = {
            "name": created_group_name,
            "service": service,
        }
        return created_group_id

    def _populate_fivetran_data(self) -> Dict[Any, Any]:
        fivetran_data = {}
        fivetran_group_map = {}
        fivetran_groups = self._fivetran_api_call(
            "GET", FIVETRAN_API_ENDPOINTS.get("DESTINATION_LIST")
        )
        for group in fivetran_groups.get("data", {}).get("items", []):
            destination_data = {}
            destination_id = group["id"]
            fivetran_group_map[destination_id] = {}
            fivetran_group_map[destination_id]["name"] = group["name"]
            destination_details = self._get_destination_details(destination_id)
            if destination_details:
                fivetran_group_map[destination_id]["service"] = destination_details["service"]
                destination_data["details"] = destination_details
                destination_data["connectors"] = self._get_destination_connectors(destination_id)
                fivetran_data[destination_id] = destination_data
        self.fivetran_groups = fivetran_group_map
        return fivetran_data

    def update_destination(self, destination_id, destination_details):
        destination = self._fivetran_api_call(
            "PATCH",
            FIVETRAN_API_ENDPOINTS["DESTINATION_DETAILS"].format(destination=destination_id),
            destination_details,
        )
        return destination["data"]

    def create_destination(self, payload):
        destination = self._fivetran_api_call(
            "POST",
            FIVETRAN_API_ENDPOINTS["DESTINATION_CREATE"],
            payload,
        )
        self.fivetran_data
        return destination["data"]

    def update_connector(self, connector_id, connector_details):
        connector = self._fivetran_api_call(
            "PATCH",
            FIVETRAN_API_ENDPOINTS["CONNECTOR_DETAILS"].format(connector=connector_id),
            connector_details,
        )
        return connector["data"]

    def create_connector(self, connector_details):
        connector = self._fivetran_api_call(
            "POST", FIVETRAN_API_ENDPOINTS["CONNECTOR_CREATE"], connector_details
        )
        return connector["data"]

    def update_connector_schema_config(self, connector_id, schemas):
        updated_schemas = self._fivetran_api_call(
            "PATCH",
            FIVETRAN_API_ENDPOINTS["CONNECTOR_SCHEMAS"].format(connector=connector_id),
            schemas,
        )
        return updated_schemas["data"]

    def get_group_name(self, group_id):
        group_data = self._fivetran_api_call(
            "GET", FIVETRAN_API_ENDPOINTS["GROUP_DETAILS"].format(group=group_id)
        )
        return group_data.get("data", {}).get("name", "")

    def get_service_required_fields(self, service_type):
        source_metadata = self._fivetran_api_call(
            "GET",
            FIVETRAN_API_ENDPOINTS["SOURCE_METADATA"].format(service=service_type),
        )
        return source_metadata.get("data", {}).get("config", {}).get("required", [])
