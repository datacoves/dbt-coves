"""Holds config for dbt-coves."""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel

from dbt_coves.utils.yaml import open_yaml
from dbt_coves.utils.flags import FlagParser
from dbt_coves.utils.log import LOGGER as logger


class DbtProjectModel(BaseModel):
    """Pydantic validation model for dbt_project dict."""

    name: str
    path: str


class ConfigModel(BaseModel):
    """Pydantic validation model for dbt_project dict."""

    schemas: Optional[str]
    dbt_project: Optional[DbtProjectModel]


class DbtCovesConfig:
    """dbt-coves configuration class."""

    COVES_CONFIG_FILENAME = ".dbt_coves"
    CLI_OVERRIDE_FLAGS = [
        # {"cli_arg_name": "schemas", "maps_to": "schemas"},
    ]

    def __init__(self, flags: FlagParser, max_dir_upwards_iterations: int = 4) -> None:
        """Constructor for DbtCovesConfig.

        Args:
            flags (FlagParser): consumed flags from FlagParser object.
        """
        self._flags = flags
        self._task = self._flags.task
        self._config_path = self._flags.config_path
        self._max_dir_upwards_iterations = max_dir_upwards_iterations
        self._current_folder = Path(self._flags.profiles_dir) if self._flags.profiles_dir else Path.cwd()
        self._config = dict()

    @property
    def config(self):
        for flag_override_dict in self.CLI_OVERRIDE_FLAGS:
            self._config[flag_override_dict["maps_to"]] = getattr(
                self._flags, flag_override_dict["cli_arg_name"]
            )
        return self._config

    @property
    def dbt_project_info(self):
        """Convenience function to ensure only one dbt project is unders scope
        This was introduced as part of an intentional regresssion because we're not ready
        to support multiple dbt projects yet.
        """
        return self.config.get("dbt_project", dict())

    def load_and_validate_config_yaml(self) -> None:
        if self._config_path:
            yaml_dict = open_yaml(self._config_path)

            # use pydantic to shape and validate
            self._config = ConfigModel(**yaml_dict)

    def locate_config(self) -> None:
        logger.debug(f"Starting config file finding from {self._current_folder}")
        current = self._current_folder
        filename = Path(current).joinpath(self.COVES_CONFIG_FILENAME)

        if self._config_path == Path(str()):
            logger.debug("Trying to find .dbt_coves file in current and parent folders")

            folder_iteration = 0
            while folder_iteration < self._max_dir_upwards_iterations:
                if filename.exists():
                    coves_config_dir = filename
                    logger.debug(f"{coves_config_dir} exists and was retreived.")
                    self._config_path = coves_config_dir
                    self._config_file_found_nearby = True
                    break
                current = current.parent
                filename = Path(current, self.COVES_CONFIG_FILENAME)
                folder_iteration += 1

    def load_config(self) -> None:
        self.locate_config()
        try:
            self.load_and_validate_config_yaml()
            logger.debug(f"Config model dict: {self.config_model.dict()}")
        except FileNotFoundError:
            logger.debug("Config file not found")