"""Holds config for dbt-coves."""

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

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
    no_prompt: Optional[bool] = False


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
    no_prompt: Optional[bool] = False
    flatten_json_fields: Optional[str] = "ask"
    overwrite_staging_models: Optional[bool] = False
    skip_model_props: Optional[bool] = False


class GenerateMetadataModel(BaseModel):
    database: Optional[str] = ""
    schemas: Optional[List[str]] = []
    select_relations: Optional[List[str]] = []
    exclude_relations: Optional[List[str]] = []
    destination: Optional[str] = "metadata.csv"
    no_prompt: Optional[bool] = False


class GenerateDocsModel(BaseModel):
    merge_deferred: Optional[bool] = False
    state: Optional[str] = ""
    dbt_args: Optional[str] = ""


class GenerateAirflowDagsModel(BaseModel):
    yml_path: Optional[str] = ""
    dags_path: Optional[str] = ""
    validate_operators: Optional[bool] = False
    generators_folder: Optional[str] = "dbt_coves.tasks.generate.airflow_generators"
    generators_params: Optional[Dict[str, Any]] = {}
    secrets_path: Optional[str] = ""
    secrets_manager: Optional[str] = ""
    secrets_url: Optional[str] = ""
    secrets_token: Optional[str] = ""
    secrets_environment: Optional[str] = ""
    secrets_tags: Optional[str] = ""
    secrets_key: Optional[str] = ""


class GenerateModel(BaseModel):
    sources: Optional[GenerateSourcesModel] = GenerateSourcesModel()
    properties: Optional[GeneratePropertiesModel] = GeneratePropertiesModel()
    metadata: Optional[GenerateMetadataModel] = GenerateMetadataModel()
    docs: Optional[GenerateDocsModel] = GenerateDocsModel()
    airflow_dags: Optional[GenerateAirflowDagsModel] = GenerateAirflowDagsModel()


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
    secrets_path: Optional[str] = ""
    secrets_manager: Optional[str] = ""
    secrets_url: Optional[str] = ""
    secrets_token: Optional[str] = ""
    secrets_environment: Optional[str] = ""
    secrets_tags: Optional[List[str]] = []
    secrets_key: Optional[str] = ""


class LoadFivetranModel(BaseModel):
    path: Optional[str] = ""
    api_key: Optional[str] = ""
    api_secret: Optional[str] = ""
    secrets_path: Optional[str] = ""
    credentials: Optional[str] = ""
    secrets_manager: Optional[str] = ""
    secrets_url: Optional[str] = ""
    secrets_token: Optional[str] = ""
    secrets_environment: Optional[str] = ""
    secrets_tags: Optional[List[str]] = []
    secrets_key: Optional[str] = ""


class ExtractModel(BaseModel):
    airbyte: Optional[ExtractAirbyteModel] = ExtractAirbyteModel()
    fivetran: Optional[ExtractFivetranModel] = ExtractFivetranModel()


class LoadModel(BaseModel):
    airbyte: Optional[LoadAirbyteModel] = LoadAirbyteModel()
    fivetran: Optional[LoadFivetranModel] = LoadFivetranModel()


class SetupModel(BaseModel):
    no_prompt: Optional[bool] = False
    quiet: Optional[bool] = False
    template_url: Optional[str] = "https://github.com/datacoves/setup_template.git"


class RunDbtModel(BaseModel):
    command: Optional[str] = ""
    project_dir: Optional[str] = ""
    virtualenv: Optional[str] = ""
    cleanup: Optional[bool] = False


class RedshiftDataSyncModel(BaseModel):
    tables: Optional[List[str]] = []


class SnowflakeDataSyncModel(BaseModel):
    tables: Optional[List[str]] = []


class DataSyncModel(BaseModel):
    redshift: Optional[RedshiftDataSyncModel] = RedshiftDataSyncModel()
    snowflake: Optional[SnowflakeDataSyncModel] = SnowflakeDataSyncModel()


class BlueGreenModel(BaseModel):
    prod_db_env_var: Optional[str] = ""
    staging_database: Optional[str] = ""
    staging_suffix: Optional[str] = ""
    drop_staging_db_at_start: Optional[bool] = False
    keep_staging_db_on_success: Optional[bool] = False
    drop_staging_db_on_failure: Optional[bool] = False
    dbt_selector: Optional[str] = ""
    defer: Optional[bool] = False
    full_refresh: Optional[bool] = False


class ConfigModel(BaseModel):
    generate: Optional[GenerateModel] = GenerateModel()
    extract: Optional[ExtractModel] = ExtractModel()
    load: Optional[LoadModel] = LoadModel()
    setup: Optional[SetupModel] = SetupModel()
    dbt: Optional[RunDbtModel] = RunDbtModel()
    data_sync: Optional[DataSyncModel] = DataSyncModel()
    disable_tracking: Optional[bool] = False
    blue_green: Optional[BlueGreenModel] = BlueGreenModel()


class DbtCovesConfig:
    """dbt-coves configuration class."""

    DBT_COVES_CONFIG_FOLDER = ".dbt_coves"
    DBT_COVES_CONFIG_FILEPATHS = [".dbt_coves/config.yml", ".dbt_coves/config.yaml"]
    CLI_OVERRIDE_FLAGS = [
        "generate.properties.templates_folder",
        "generate.properties.metadata",
        "generate.properties.destination",
        "generate.properties.update_strategy",
        "generate.properties.select",
        "generate.properties.exclude",
        "generate.properties.selector",
        "generate.properties.no_prompt",
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
        "generate.sources.no_prompt",
        "generate.sources.flatten_json_fields",
        "generate.sources.overwrite_staging_models",
        "generate.sources.skip_model_props",
        "generate.metadata.database",
        "generate.metadata.schemas",
        "generate.metadata.select_relations",
        "generate.metadata.exclude_relations",
        "generate.metadata.destination",
        "generate.metadata.no_prompt",
        "generate.docs.merge_deferred",
        "generate.docs.state",
        "generate.docs.dbt_args",
        "generate.airflow_dags.yml_path",
        "generate.airflow_dags.dags_path",
        "generate.airflow_dags.validate_operators",
        "generate.airflow_dags.generators_folder",
        "generate.airflow_dags.generators_params",
        "generate.airflow_dags.secrets_path",
        "generate.airflow_dags.secrets_manager",
        "generate.airflow_dags.secrets_url",
        "generate.airflow_dags.secrets_token",
        "generate.airflow_dags.secrets_environment",
        "generate.airflow_dags.secrets_tags",
        "generate.airflow_dags.secrets_key",
        "extract.airbyte.path",
        "extract.airbyte.host",
        "extract.airbyte.port",
        "load.airbyte.path",
        "load.airbyte.host",
        "load.airbyte.port",
        "load.airbyte.secrets_path",
        "load.airbyte.secrets_manager",
        "load.airbyte.secrets_url",
        "load.airbyte.secrets_token",
        "load.airbyte.secrets_environment",
        "load.airbyte.secrets_tags",
        "load.airbyte.secrets_key",
        "setup.no_prompt",
        "setup.quiet",
        "setup.template_url",
        "dbt.command",
        "dbt.project_dir",
        "dbt.virtualenv",
        "dbt.cleanup",
        "extract.fivetran.path",
        "extract.fivetran.api_key",
        "extract.fivetran.api_secret",
        "extract.fivetran.credentials",
        "load.fivetran.path",
        "load.fivetran.api_key",
        "load.fivetran.api_secret",
        "load.fivetran.secrets_path",
        "load.fivetran.credentials",
        "load.fivetran.secrets_manager",
        "load.fivetran.secrets_url",
        "load.fivetran.secrets_token",
        "load.fivetran.secrets_environment",
        "load.fivetran.secrets_tags",
        "load.fivetran.secrets_key",
        "data_sync.redshift.tables",
        "data_sync.snowflake.tables",
        "blue_green.prod_db_env_var",
        "blue_green.staging_database",
        "blue_green.staging_suffix",
        "blue_green.drop_staging_db_at_start",
        "blue_green.drop_staging_db_on_failure",
        "blue_green.dbt_selector",
        "blue_green.defer",
        "blue_green.full_refresh",
        "blue_green.keep_staging_db_on_success",
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

    @property
    def disable_tracking(self):
        return self._config.disable_tracking

    def load_and_validate_config_yaml(self) -> None:
        if self._config_path:
            yaml_dict = open_yaml(self._config_path) or {}

            # Replace environment variable placeholders
            yaml_dict = self.replace_env_vars(yaml_dict)

            # use pydantic to shape and validate
            self._config = ConfigModel(**yaml_dict)

    def replace_env_vars(self, yaml_dict: Dict) -> Dict:
        # regex -> {{ env_var('ENV_VAR_NAME', 'DEFAULT_VALUE') }}
        env_var_pattern = re.compile(
            r"\{\{\s*env_var\s*\(\s*['\"]?\s*([^'\"]+)['\"]?\s*(?:,\s*['\"]?\s*([^'\"]+)['\"]?\s*)?\)\s*\}\}"
        )

        # Iterate through the YAML dictionary and replace placeholders
        def replace_env_var(match):
            env_var_name = match.group(1)
            default_value = match.group(2) or ""
            return os.environ.get(env_var_name, default_value)

        def replace_recursively(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    obj[key] = replace_recursively(value)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    obj[i] = replace_recursively(item)
            elif isinstance(obj, str):
                obj = env_var_pattern.sub(replace_env_var, obj)
            return obj

        return replace_recursively(yaml_dict)

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

            for filename in self.DBT_COVES_CONFIG_FILEPATHS:
                config_path = Path(os.environ.get("DATACOVES__DBT_HOME", "")).joinpath(filename)
                if config_path.exists():
                    coves_config_dir = config_path
                    logger.debug(f"{coves_config_dir} exists and was retrieved.")
                    self._config_path = coves_config_dir
                    break

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
