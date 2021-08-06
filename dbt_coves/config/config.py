"""Holds config for dbt-coves."""

from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel

from dbt_coves.core.exceptions import MissingDbtProject
from dbt_coves.utils.flags import DbtCovesFlags
from dbt_coves.utils.log import LOGGER as logger
from dbt_coves.utils.yaml import open_yaml


class GenerateSourcesModel(BaseModel):
    relations: Optional[List[str]] = [""]
    schemas: Optional[List[str]] = ["raw"]
    destination: Optional[str] = "models/sources/{{schema}}/{{relation}}.sql"
    model_props_strategy: Optional[str] = "one_file_per_model"
    templates_folder: Optional[str] = "templates"


class GenerateModel(BaseModel):
    sources: Optional[GenerateSourcesModel] = GenerateSourcesModel()


class ConfigModel(BaseModel):
    generate: Optional[GenerateModel] = GenerateModel()


class DbtCovesConfig:
    """dbt-coves configuration class."""

    DBT_COVES_CONFIG_FILENAME = ".dbt_coves.yml"
    CLI_OVERRIDE_FLAGS = [
        "generate.sources.relations",
        "generate.sources.schemas",
        "generate.sources.destination",
        "generate.sources.model_props_strategy",
        "generate.sources.templates_folder",
    ]

    def __init__(self, flags: DbtCovesFlags) -> None:
        """Constructor for DbtCovesConfig.

        Args:
            flags (DbtCovesFlags): consumed flags from DbtCovesFlags object.
        """
        self._flags = flags
        self._task = self._flags.task
        self._config_path = self._flags.config_path
        self._config = ConfigModel()

    @property
    def integrated(self):
        """
        Returns the values read from the config file plus the overrides from cli flags
        """
        config_copy = self._config.dict()
        for value in self.CLI_OVERRIDE_FLAGS:
            path_items = value.split(".")
            target = config_copy
            source = self._flags
            for item in path_items[:-1]:
                target = target[item]
                source = source[item] if type(source) == dict else getattr(source, item)
            key = path_items[-1]
            if source[key]:
                target[key] = source[key]
        return config_copy

    def load_and_validate_config_yaml(self) -> None:
        if self._config_path:
            yaml_dict = open_yaml(self._config_path)

            # use pydantic to shape and validate
            self._config = ConfigModel(**yaml_dict)

    def locate_config(self) -> None:
        dbt_project = Path().joinpath("dbt_project.yml")
        if dbt_project.exists():
            if self._config_path == Path(str()):
                logger.debug("Trying to find .dbt_coves file in current folder")

                filename = Path().joinpath(self.DBT_COVES_CONFIG_FILENAME)
                if filename.exists():
                    coves_config_dir = filename
                    logger.debug(f"{coves_config_dir} exists and was retreived.")
                    self._config_path = coves_config_dir
                    self._config_file_found_nearby = True
        else:
            raise MissingDbtProject()

    def load_config(self) -> None:
        self.locate_config()
        try:
            self.load_and_validate_config_yaml()
            logger.debug(f"Config model dict: {self._config.dict()}")
        except FileNotFoundError:
            logger.debug("Config file not found")
