"""Holds methods to interact with dbt API (we mostly don't for now because not stable) and objects."""
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union

from pydantic import BaseModel

from dbt_coves.utils.yaml import open_yaml
from dbt_coves.utils.flags import MainParser
from dbt_coves.utils.log import LOGGER as logger
from . import _assert_file_exists


DEFAULT_DBT_PROFILE_PATH = Path(os.getenv("DBT_PROFILES_DIR", default=Path.home().joinpath(".dbt")))


class DbtProjectModel(BaseModel):
    """Defines pydandic validation schema for a dbt_project.yml file."""

    profile: str


class DbtProject:
    """Holds parsed dbt project information needed for dbt-coves such as which db profile to target."""

    DBT_PROJECT_FILENAME: str = "dbt_project.yml"

    def __init__(self, project_name: str, project_dir: Path) -> None:
        """Constructor for DbtProject.

        Given a project name and a project dir it will parse the relevant dbt_project.yml and
        parse information such as `profile` so dbt-coves knows which database profile entry from
        /.dbt/profiles.yml to use.

        Args:
            project_name (str): Name of the dbt project to read profile from.
            project_dir (Path): Path object the dbt_project.yml to read from.
        """
        self._project_name = project_name
        self._project_dir = project_dir

        # class "outputs"
        self.project: DbtProjectModel
        self.profile_name: str

    @property
    def _dbt_project_filename(self) -> Path:
        logger.debug(f"project_dir: {self._project_dir}")
        return Path(self._project_dir).joinpath(type(self).DBT_PROJECT_FILENAME)

    def read_project(self) -> None:
        _ = _assert_file_exists(
            Path(self._project_dir), filename=self.DBT_PROJECT_FILENAME)
        _project_dict = open_yaml(self._dbt_project_filename)

        # pass the dict through pydantic for validation and only getting what we need
        # if the profile is invalid app will crash so no further tests required below.
        logger.debug(f"the project {_project_dict}")
        _project = DbtProjectModel(**_project_dict)
        logger.debug(_project)
        self.project = _project
        self.profile_name = self.project.dict().get("profile", str())

        if not self.profile_name:
            logger.warning(
                f"[yellow]There was no `profile:` entry in {self._dbt_project_filename}. "
                "dbt-coves will try to find a 'default' profile. This might lead to unexpected"
                "behaviour or an error when no defaulf profile can be found in your dbt profiles.yml"
            )