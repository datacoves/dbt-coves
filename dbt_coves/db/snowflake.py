from typing import Dict, Sequence

import sqlalchemy
from snowflake.sqlalchemy import URL

from dbt_coves.db import BaseConnector
from dbt_coves.utils.log import LOGGER as logger


class SnowflakeConnector(BaseConnector):
    def __init__(
        self,
        connection_params: Dict[str, str],
    ) -> None:
        self.connection_url = URL(
            drivername="postgresql+psycopg2",
            user=connection_params.get("user", str()),
            password=connection_params.get("password", str()),
            database=connection_params.get("database", str()),
            account=connection_params.get("account", str()),
            warehouse=connection_params.get("warehouse", str()),
        )
        self.engine = sqlalchemy.create_engine(self.connection_url)
