from collections import OrderedDict
from typing import Any, Dict, List

from slugify import slugify

from dbt_coves.tasks.generate.airflow_generators.base import (
    BaseDbtCovesTaskGenerator,
    BaseDbtGenerator,
)
from dbt_coves.utils.api_caller import AirbyteApiCaller


class AirbyteGeneratorException(Exception):
    pass


class AirbyteGenerator(BaseDbtCovesTaskGenerator):
    def __init__(
        self,
        host: str = "http://localhost",
        port: str = "8000",
        connection_ids: List[str] = [],
        airbyte_conn_id: str = "",
    ):
        self.host = host
        self.port = port
        self.airbyte_conn_id = airbyte_conn_id
        self.connection_ids = connection_ids
        self.ignored_source_tables = []
        self.imports = ["airflow.providers.airbyte.operators.airbyte.AirbyteTriggerSyncOperator"]
        self.api_caller = AirbyteApiCaller(self.host, self.port)
        self.airbyte_connections = self.api_caller.airbyte_connections_list
        self.connections_should_exist = False

    def validate_ids_in_airbyte(self, connection_ids):
        """
        Ensure connection_ids exist in Airbyte API
        """
        for conn in connection_ids:
            if conn not in (connection["connectionId"] for connection in self.airbyte_connections):
                raise AirbyteGeneratorException(
                    f"Airbyte error: there is no Airbyte connection for id [red]{conn}[/red]"
                )

    def generate_tasks(self) -> Dict[str, Any]:
        """
        Return "variable = call" strings of Airflow Airbyte code
        """
        self.validate_ids_in_airbyte(self.connection_ids)
        tasks = OrderedDict()
        for conn_id in self.connection_ids:
            task_name = self._create_airbyte_connection_name_for_id(conn_id)
            operator_kwargs = {
                "task_id": task_name,
                "connection_id": conn_id,
                "airbyte_conn_id": self.airbyte_conn_id,
            }
            tasks[task_name] = self.generate_task(
                task_name, "AirbyteTriggerSyncOperator", **operator_kwargs
            )
        return tasks

    def _get_airbyte_destination(self, id):
        """Given a destination id, returns the destination payload"""
        for destination in self.api_caller.airbyte_destinations_list:
            if destination["destinationId"] == id:
                return destination
        raise AirbyteGeneratorException(f"Airbyte error: there are no destinations for id {id}")

    def _get_airbyte_source(self, id):
        """Get the complete Source object from it's ID"""
        for source in self.api_caller.airbyte_sources_list:
            if source["sourceId"] == id:
                return source
        raise AirbyteGeneratorException(
            f"Airbyte extract error: there is no Airbyte Source for id [red]{id}[/red]"
        )

    def _get_connection_schema(self, conn, destination_config):
        """Given an airybte connection, returns a schema name"""
        namespace_definition = conn["namespaceDefinition"]

        if namespace_definition == "source" or (
            conn["namespaceDefinition"] == "customformat"
            and conn["namespaceFormat"] == "${SOURCE_NAMESPACE}"
        ):
            source = self._get_airbyte_source(conn["sourceId"])
            if "schema" in source["connectionConfiguration"]:
                return source["connectionConfiguration"]["schema"].lower()
            else:
                return None
        elif namespace_definition == "destination":
            return destination_config["schema"].lower()
        else:
            if namespace_definition == "customformat":
                return conn["namespaceFormat"].lower()

    def get_pipeline_connection_ids(self, db: str, schema: str, table: str) -> str:
        """
        Given a table name, schema and db, returns the corresponding Airbyte Connection ID
        """
        airbyte_tables = []
        connection_ids = []
        for conn in list(
            filter(lambda conn: conn.get("status") == "active", self.airbyte_connections)
        ):
            for stream in conn["syncCatalog"]["streams"]:
                # look for the table
                airbyte_table = stream["stream"]["name"].lower()
                airbyte_tables.append(airbyte_table)
                if airbyte_table == table.replace("_airbyte_raw_", ""):
                    destination_config = self._get_airbyte_destination(conn["destinationId"])[
                        "connectionConfiguration"
                    ]

                    # match database
                    if (
                        db
                        == destination_config.get(
                            "database", destination_config.get("project-id", "")
                        ).lower()
                    ):
                        airbyte_schema = self._get_connection_schema(conn, destination_config)
                        # and finally, match schema, if defined
                        if (airbyte_schema == schema or not airbyte_schema) and conn.get(
                            "connectionId"
                        ) not in connection_ids:
                            connection_ids.append(conn["connectionId"])
        if connection_ids:
            return connection_ids
        if self.connections_should_exist:
            raise AirbyteGeneratorException(
                f"Airbyte error: there are no connections for table {db}.{schema}.{table}. "
                f"Tables checked: {', '.join(airbyte_tables)}"
            )

    def _create_airbyte_connection_name_for_id(self, conn_id):
        """
        Given a ConnectionID, create it's name using both Source and Destination ones
        """
        for conn in self.airbyte_connections:
            if conn["connectionId"] == conn_id:
                source_name = self._get_airbyte_source(conn["sourceId"])["name"]
                destination_name = self._get_airbyte_destination(conn["destinationId"])["name"]
                return slugify(f"{source_name} → {destination_name}", separator="_")

        raise AirbyteGeneratorException(
            f"Airbyte error: there are missing names for connection ID {conn_id}"
        )


class AirbyteDbtGenerator(AirbyteGenerator, BaseDbtGenerator):
    def __init__(
        self,
        host: str = "http://localhost",
        port: str = "8000",
        dbt_project_path: str = "",
        virtualenv_path: str = "",
        run_dbt_compile: bool = False,
        dbt_list_args: str = "",
        run_dbt_deps: bool = False,
        airbyte_conn_id: str = "",
    ):
        AirbyteGenerator.__init__(self, host=host, port=port, airbyte_conn_id=airbyte_conn_id)
        BaseDbtGenerator.__init__(
            self,
            dbt_project_path,
            virtualenv_path,
            run_dbt_compile,
            run_dbt_deps,
            dbt_list_args,
        )
        self.connection_ids = self.discover_dbt_connections()
