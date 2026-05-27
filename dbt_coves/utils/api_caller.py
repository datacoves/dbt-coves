import json
from typing import Any, Dict

import requests
from requests.auth import HTTPBasicAuth
from requests.exceptions import RequestException
from rich.console import Console

console = Console()

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
    "CONNECTOR_TABLES_CONFIG": FIVETRAN_API_BASE_URL
    + "/connectors/{connector}/schemas/{schema}/tables/{table}",
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
    """
    API caller for Airbyte's public REST API (/api/public/v1).
    Replaces the old internal config API (/api/v1) which used POST for all operations.
    """

    def __init__(self, api_host, api_port=None, api_key=None):
        host = api_host.rstrip("/")
        if api_port:
            host = f"{host}:{api_port}"
        self.base_url = f"{host}/api/public/v1"
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json",
        }
        if api_key:
            self.headers["authorization"] = f"Bearer {api_key}"

        try:
            console.print("Querying [i]Airbyte[/i] connections")
            workspaces = self._get_all("workspaces")
            if not workspaces:
                raise AirbyteApiCallerException("No Airbyte workspaces found")
            self.workspace_id = workspaces[0]["workspaceId"]
            self.connections_list = self._get_all("connections", workspaceIds=self.workspace_id)
            self.sources_list = self._get_all("sources", workspaceIds=self.workspace_id)
            self.destinations_list = self._get_all("destinations", workspaceIds=self.workspace_id)
            self.load_definitions()
        except AirbyteApiCallerException as e:
            raise AirbyteApiCallerException(
                f"Couldn't retrieve Airbyte connections, sources and destinations: {e}"
            )

    def _request(self, method, path, body=None, params=None, timeout=None):
        url = f"{self.base_url}/{path.lstrip('/')}"
        response = requests.request(
            method, url=url, json=body, params=params, headers=self.headers, timeout=timeout
        )
        if response.status_code == 204:
            return None
        if 200 <= response.status_code < 300:
            return response.json() if response.text else None
        try:
            message = response.json().get("message", response.text)
        except Exception:
            message = response.text
        raise AirbyteApiCallerException(
            f"Airbyte API error ({response.status_code}) at {path}: {message}"
        )

    def _get_all(self, resource, **params):
        """Fetch all pages from a list endpoint, handling pagination."""
        results = []
        limit = 100
        offset = 0
        while True:
            page = self._request(
                "GET", resource, params={"limit": limit, "offset": offset, **params}
            )
            data = page.get("data", [])
            results.extend(data)
            if len(data) < limit:
                break
            offset += limit
        return results

    def load_definitions(self):
        self.destination_definitions = self._get_all(
            "connector_definitions/destinations", workspaceId=self.workspace_id
        )
        self.source_definitions = self._get_all(
            "connector_definitions/sources", workspaceId=self.workspace_id
        )

    def get_source_spec(self, definition_id):
        """Fetch connector spec (including airbyte_secret markers) for a source definition."""
        try:
            return self._request("GET", f"connector_definitions/sources/{definition_id}")
        except AirbyteApiCallerException:
            return None

    def get_destination_spec(self, definition_id):
        """Fetch connector spec for a destination definition."""
        try:
            return self._request("GET", f"connector_definitions/destinations/{definition_id}")
        except AirbyteApiCallerException:
            return None

    # Source CRUD
    def create_source(self, body):
        return self._request("POST", "sources", body=body)

    def update_source(self, source_id, body):
        return self._request("PATCH", f"sources/{source_id}", body=body)

    def delete_source(self, source_id):
        self._request("DELETE", f"sources/{source_id}")

    def check_source(self, source_id, timeout=None):
        return self._request("POST", f"sources/{source_id}/check", timeout=timeout)

    # Destination CRUD
    def create_destination(self, body):
        return self._request("POST", "destinations", body=body)

    def update_destination(self, destination_id, body):
        return self._request("PATCH", f"destinations/{destination_id}", body=body)

    def delete_destination(self, destination_id):
        self._request("DELETE", f"destinations/{destination_id}")

    def check_destination(self, destination_id, timeout=None):
        return self._request("POST", f"destinations/{destination_id}/check", timeout=timeout)

    # Connection CRUD
    def create_connection(self, body):
        return self._request("POST", "connections", body=body)

    def update_connection(self, connection_id, body):
        return self._request("PATCH", f"connections/{connection_id}", body=body)

    def delete_connection(self, connection_id):
        self._request("DELETE", f"connections/{connection_id}")


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
        console.print("Querying [i]Fivetran[/i] connections")
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
                fivetran_group_map[destination_id]["service"] = destination_details.get(
                    "service", ""
                )
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
        updated_schemas["data"]

    def update_connector_table_config(
        self,
        connector_id,
        schema_name_in_destination,
        table_name_in_destination,
        table_config,
    ):
        self._fivetran_api_call(
            "PATCH",
            FIVETRAN_API_ENDPOINTS["CONNECTOR_TABLES_CONFIG"].format(
                connector=connector_id,
                schema=schema_name_in_destination,
                table=table_name_in_destination,
            ),
            table_config,
        )

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
