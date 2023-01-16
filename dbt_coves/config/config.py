"""Holds config for dbt-coves."""

from pathlib import Path
from typing import List, Optional

from pydantic import BaseModel

from dbt_coves.core.exceptions import MissingDbtProject
from dbt_coves.utils.flags import DbtCovesFlags
from dbt_coves.utils.log import LOGGER as logger
from dbt_coves.utils.yaml import open_yaml


class GeneratePropertiesModel(BaseModel):
    templates_folder: Optional[str] = ".dbt_coves/templates"
    metadata: Optional[str] = ""
    update_strategy: Optional[str] = "ask"
    destination: Optional[str] = "{{model_folder_path}}/{{model_file_name}}.yml"
    select: Optional[str] = ""
    exclude: Optional[str] = ""
    selector: Optional[str] = ""


class GenerateSourcesModel(BaseModel):
    database: Optional[str] = ""
    select_relations: Optional[List[str]] = []
    schemas: Optional[List[str]] = []
    exclude_relations: Optional[List[str]] = []
    sources_destination: Optional[str] = "models/staging/{{schema}}/{{schema}}.yml"
    models_destination: Optional[str] = "models/staging/{{schema}}/{{relation}}.sql"
    model_props_destination: Optional[str] = "models/staging/{{schema}}/{{relation}}.yml"
    update_strategy: Optional[str] = "ask"
    templates_folder: Optional[str] = ".dbt_coves/templates"
    metadata: Optional[str] = ""


class GenerateMetadataModel(BaseModel):
    database: Optional[str] = ""
    schemas: Optional[List[str]] = []
    select_relations: Optional[List[str]] = []
    exclude_relations: Optional[List[str]] = []
    destination: Optional[str] = "metadata.csv"


class GenerateModel(BaseModel):
    sources: Optional[GenerateSourcesModel] = GenerateSourcesModel()
    properties: Optional[GeneratePropertiesModel] = GeneratePropertiesModel()
    metadata: Optional[GenerateMetadataModel] = GenerateMetadataModel()


class ExtractAirbyteModel(BaseModel):
    path: Optional[str] = ""
    host: Optional[str] = ""
    port: Optional[str] = ""


class ExtractFivetranModel(BaseModel):
    path: Optional[str] = ""
    api_key: Optional[str] = ""
    api_secret: Optional[str] = ""
    credentials: Optional[str] = ""


class LoadAirbyteModel(BaseModel):
    path: Optional[str] = ""
    host: Optional[str] = ""
    port: Optional[str] = ""
    secrets_manager: Optional[str] = ""
    secrets_url: Optional[str] = ""
    secrets_token: Optional[str] = ""
    secrets_path: Optional[str] = ""


class LoadFivetranModel(BaseModel):
    path: Optional[str] = ""
    api_key: Optional[str] = ""
    api_secret: Optional[str] = ""
    secrets_path: Optional[str] = ""
    credentials: Optional[str] = ""


class ExtractModel(BaseModel):
    airbyte: Optional[ExtractAirbyteModel] = ExtractAirbyteModel()
    fivetran: Optional[ExtractFivetranModel] = ExtractFivetranModel()


class LoadModel(BaseModel):
    airbyte: Optional[LoadAirbyteModel] = LoadAirbyteModel()
    fivetran: Optional[LoadFivetranModel] = LoadFivetranModel()


class SetupAllModel(BaseModel):
    open_ssl_public_key: Optional[bool] = False


class SetupSshModel(BaseModel):
    open_ssl_public_key: Optional[bool] = False


class SetupGitModel(BaseModel):
    no_prompt: Optional[bool] = False


class SetupModel(BaseModel):
    all: Optional[SetupAllModel] = SetupAllModel()
    ssh: Optional[SetupSshModel] = SetupSshModel()
    git: Optional[SetupGitModel] = SetupGitModel()


class RunDbtModel(BaseModel):
    command: Optional[str] = ""
    project_dir: Optional[str] = ""
    virtualenv: Optional[str] = ""


class ConfigModel(BaseModel):
    generate: Optional[GenerateModel] = GenerateModel()
    extract: Optional[ExtractModel] = ExtractModel()
    load: Optional[LoadModel] = LoadModel()
    setup: Optional[SetupModel] = SetupModel()
    dbt: Optional[RunDbtModel] = RunDbtModel()


class DbtCovesConfig:
    """dbt-coves configuration class."""

    DBT_COVES_CONFIG_FILEPATH = ".dbt_coves/config.yml"
    CLI_OVERRIDE_FLAGS = [
        "generate.properties.templates_folder",
        "generate.properties.metadata",
        "generate.properties.destination",
        "generate.properties.update_strategy",
        "generate.properties.select",
        "generate.properties.exclude",
        "generate.properties.selector",
        "generate.sources.select_relations",
        "generate.sources.exclude_relations",
        "generate.sources.database",
        "generate.sources.schemas",
        "generate.sources.sources_destination",
        "generate.sources.models_destination",
        "generate.sources.model_props_destination",
        "generate.sources.update_strategy",
        "generate.sources.templates_folder",
        "generate.sources.metadata",
        "generate.metadata.database",
        "generate.metadata.schemas",
        "generate.metadata.select_relations",
        "generate.metadata.exclude_relations",
        "generate.metadata.destination",
        "extract.airbyte.path",
        "extract.airbyte.host",
        "extract.airbyte.port",
        "load.airbyte.path",
        "load.airbyte.host",
        "load.airbyte.port",
        "load.airbyte.secrets_manager",
        "load.airbyte.secrets_url",
        "load.airbyte.secrets_token",
        "load.airbyte.secrets_path",
        "setup.all.open_ssl_public_key",
        "setup.ssh.open_ssl_public_key",
        "setup.git.no_prompt",
        "dbt.command",
        "dbt.project_dir",
        "dbt.virtualenv",
        "extract.fivetran.path",
        "extract.fivetran.api_key",
        "extract.fivetran.api_secret",
        "extract.fivetran.credentials",
        "load.fivetran.path",
        "load.fivetran.api_key",
        "load.fivetran.api_secret",
        "load.fivetran.secrets_path",
        "load.fivetran.credentials",
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
                source = source.get(item, {}) if type(source) == dict else getattr(source, item)
            key = path_items[-1]
            if source.get(key):
                target[key] = source[key]
        return config_copy

    def load_and_validate_config_yaml(self) -> None:
        if self._config_path:
            yaml_dict = open_yaml(self._config_path) or {}

            # use pydantic to shape and validate
            self._config = ConfigModel(**yaml_dict)

    def validate_dbt_project(self):
        if not self._flags.task_cls.needs_dbt_project:
            return True
        if self._flags.project_dir:
            dbt_project = Path(self._flags.project_dir).joinpath("dbt_project.yml")
        else:
            dbt_project = Path().joinpath("dbt_project.yml")
        return dbt_project.exists()

    def locate_config(self) -> None:
        # If path is relative to cwd
        if self._config_path == Path(str()):
            logger.debug("Trying to find .dbt_coves in current folder")

            config_path = Path().joinpath(self.DBT_COVES_CONFIG_FILEPATH)
            if config_path.exists():
                coves_config_dir = config_path
                logger.debug(f"{coves_config_dir} exists and was retreived.")
                self._config_path = coves_config_dir

    def load_config(self) -> None:
        is_project_valid = self.validate_dbt_project()
        if is_project_valid:
            self.locate_config()
        else:
            raise MissingDbtProject()
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
