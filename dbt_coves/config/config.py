"""Holds config for dbt-coves."""

from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel

from dbt_coves.core.exceptions import MissingDbtProject
from dbt_coves.utils.flags import DbtCovesFlags
from dbt_coves.utils.log import LOGGER as logger
from dbt_coves.utils.yaml import open_yaml


class GenerateSourcesModel(BaseModel):
    database: Optional[str] = ""
    relations: Optional[List[str]] = [""]
    schemas: Optional[List[str]] = ["raw"]
    destination: Optional[str] = "models/sources/{{schema}}/{{relation}}.sql"
    model_props_strategy: Optional[str] = "one_file_per_model"
    templates_folder: Optional[str] = ".dbt_coves/templates"


class GenerateModel(BaseModel):
    sources: Optional[GenerateSourcesModel] = GenerateSourcesModel()


class ExtractAirbyteModel(BaseModel):
    path: Optional[str] = ""
    host: Optional[str] = ""
    port: Optional[str] = ""
    dbt_list_args: Optional[str] = ""


class LoadAirbyteModel(BaseModel):
    path: Optional[str] = ""
    host: Optional[str] = ""
    port: Optional[str] = ""
    secrets: Optional[str] = ""


class ExtractModel(BaseModel):
    airbyte: Optional[ExtractAirbyteModel] = ExtractAirbyteModel()


class LoadModel(BaseModel):
    airbyte: Optional[LoadAirbyteModel] = LoadAirbyteModel()


class ConfigModel(BaseModel):
    generate: Optional[GenerateModel] = GenerateModel()
    extract: Optional[ExtractModel] = ExtractModel()
    load: Optional[LoadModel] = LoadModel()


class DbtCovesConfig:
    """dbt-coves configuration class."""

    DBT_COVES_CONFIG_FILENAMES = [".dbt_coves.yml", ".dbt_coves/config.yml"]
    CLI_OVERRIDE_FLAGS = [
        "generate.sources.relations",
        "generate.sources.database",
        "generate.sources.schemas",
        "generate.sources.destination",
        "generate.sources.model_props_strategy",
        "generate.sources.templates_folder",
        "extract.airbyte.path",
        "extract.airbyte.host",
        "extract.airbyte.port",
        "extract.airbyte.dbt_list_args",
        "load.airbyte.path",
        "load.airbyte.host",
        "load.airbyte.port",
        "load.airbyte.secrets",
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
                logger.debug("Trying to find .dbt_coves in current folder")

                for tentative_path in self.DBT_COVES_CONFIG_FILENAMES:
                    config_path = Path().joinpath(tentative_path)
                    if config_path.exists():
                        coves_config_dir = config_path
                        logger.debug(f"{coves_config_dir} exists and was retreived.")
                        self._config_path = coves_config_dir
                        break
        else:
            raise MissingDbtProject()

    def load_config(self) -> None:
        self.locate_config()
        try:
            self.load_and_validate_config_yaml()
            logger.debug(f"Config model dict: {self._config.dict()}")
        except FileNotFoundError:
            logger.debug("Config file not found")

    @classmethod
    def get_config_folder(cls, workspace_path=None, mandatory=True):
        if not workspace_path:
            workspace_path = Path.cwd()
        config_folders = [path for path in Path(workspace_path).rglob("**/.dbt_coves/")]
        if not config_folders:
            if mandatory:
                raise Exception("No .dbt_coves folder found in workspace")
            else:
                return None
        else:
            return config_folders[0]
