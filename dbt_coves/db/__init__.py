from abc import ABC
from typing import Dict, Sequence

import sqlalchemy


class BaseConnector(ABC):
    def __init__(
        self,
        connection_params: Dict[str, str],
    ) -> None:
        self.engine = sqlalchemy.create_engine(**connection_params)
