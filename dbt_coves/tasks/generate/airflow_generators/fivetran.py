from collections import OrderedDict
from typing import Any, Dict, List

from slugify import slugify

from dbt_coves.utils.api_caller import FivetranApiCaller

from .base import BaseDbtCovesTaskGenerator, BaseDbtGenerator


class FivetranGeneratorException(Exception):
    pass


class FivetranGenerator(BaseDbtCovesTaskGenerator):
    def __init__(
        self,
        api_key: str,
        api_secret: str,
        wait_for_completion: bool,
        connection_ids: List[str] = [],
        fivetran_conn_id: str = "",
    ):
        self.imports = [
            "fivetran_provider.operators.fivetran.FivetranOperator",
            "fivetran_provider.sensors.fivetran.FivetranSensor",
        ]
        self.connection_ids = connection_ids
        self.wait_for_completion = wait_for_completion
        self.ignored_source_tables = ["fivetran_audit", "fivetran_audit_warning"]
        self.fivetran_api = FivetranApiCaller(api_key, api_secret)
        self.fivetran_data = self.fivetran_api.fivetran_data
        self.fivetran_groups = self.fivetran_api.fivetran_groups
        self.connectors_should_exist = False
        self.fivetran_conn_id = fivetran_conn_id

    def _get_fivetran_connector_name_for_id(self, connector_id):
        """
        Create a name for Fivetran tasks based on a Connector ID
        """
        for dest_data in self.fivetran_data.values():
            for connector_data in dest_data.get("connectors", {}).values():
                details = connector_data["details"]
                if details["id"] == connector_id:
                    return slugify(
                        f"{self.fivetran_groups[details['group_id']]['name']}.{details['schema']}",
                        separator="_",
                    )

    def generate_tasks(self) -> Dict[str, Any]:
        """
        Return "variable = call" strings of Airflow Fivetran code
        """
        tasks = OrderedDict()
        for conn_id in self.connection_ids:
            task_name = self._get_fivetran_connector_name_for_id(conn_id)

            trigger_id = task_name + "_trigger"
            trigger_kwargs = {
                "task_id": trigger_id,
                "connector_id": conn_id,
                "do_xcom_push": True,
                "fivetran_conn_id": self.fivetran_conn_id,
            }
            tasks[conn_id] = {
                "trigger": {
                    "name": trigger_id,
                    "call": self.generate_task(trigger_id, "FivetranOperator", **trigger_kwargs),
                }
            }
            if self.wait_for_completion:
                # Sensor task - senses Fivetran connectors status
                sensor_id = task_name + "_sensor"
                sensor_kwargs = {
                    "task_id": sensor_id,
                    "connector_id": conn_id,
                    "poke_interval": 60,
                    "fivetran_conn_id": self.fivetran_conn_id,
                }
                tasks[conn_id]["sensor"] = {
                    "name": sensor_id,
                    "call": self.generate_task(sensor_id, "FivetranSensor", **sensor_kwargs),
                }

        return tasks

    def _dbt_database_in_destination(self, fivetran_destination, dbt_database):
        return (
            dbt_database
            == fivetran_destination.get("details").get("config", {}).get("database", "").lower()
        )

    def _dbt_schema_table_in_connector(self, connector_schemas, dbt_schema, dbt_table):
        for schema_details in connector_schemas.values():
            if schema_details.get("name_in_destination", "").lower() == dbt_schema:
                for table_details in schema_details.get("tables", {}).values():
                    if table_details.get("name_in_destination", "").lower() == dbt_table:
                        return True
        return False

    def get_pipeline_connection_ids(
        self, source_db: str, source_schema: str, source_table: str
    ) -> str:
        """
        Given a table name, schema and db, returns the corresponding Fivetran Connection ID
        """
        fivetran_schema_db_naming = f"{source_schema}.{source_table}".lower()
        connector_ids = []
        for dest_dict in self.fivetran_data.values():
            # destination dict can be empty if Fivetran Destination is missing configuration or not yet tested
            if dest_dict and dest_dict.get("details"):
                # match dbt source_db to Fivetran destination database
                if self._dbt_database_in_destination(dest_dict, source_db.lower()):
                    # find the appropiate Connector from destination connectors)
                    for connector_id, connector_data in dest_dict.get("connectors", {}).items():
                        for schema_id, schema_data in connector_data.get("schemas", {}).items():
                            if (
                                self._dbt_schema_table_in_connector(
                                    {schema_id: schema_data},
                                    source_schema.lower(),
                                    source_table.lower(),
                                )
                                and connector_id not in connector_ids
                            ):
                                connector_ids.append(connector_id)
        if connector_ids:
            return connector_ids
        if self.connectors_should_exist:
            raise FivetranGeneratorException(
                f"There is no Fivetran Connector for {source_db}.{fivetran_schema_db_naming}"
            )


class FivetranDbtGenerator(FivetranGenerator, BaseDbtGenerator):
    def __init__(
        self,
        api_key,
        api_secret,
        wait_for_completion: bool = True,
        dbt_project_path: str = "",
        virtualenv_path: str = "",
        run_dbt_compile: bool = False,
        dbt_list_args: str = "",
        run_dbt_deps: bool = False,
        fivetran_conn_id: str = "",
        connection_ids: List[str] = [],
    ) -> Dict[str, Any]:
        FivetranGenerator.__init__(
            self,
            api_key=api_key,
            api_secret=api_secret,
            wait_for_completion=wait_for_completion,
            fivetran_conn_id=fivetran_conn_id,
            connection_ids=connection_ids,
        )
        BaseDbtGenerator.__init__(
            self,
            dbt_project_path,
            virtualenv_path,
            run_dbt_compile,
            run_dbt_deps,
            dbt_list_args,
        )
        self.connection_ids = self.discover_dbt_connections()
