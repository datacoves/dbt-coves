from pathlib import Path
from dbt_coves.utils.log import LOGGER as logger
from dbt_coves.core.exceptions import DbtProfileFileMissing


def _assert_file_exists(dir: Path, filename: str = "profiles.yml") -> bool:
    logger.debug(dir.resolve())
    full_path_to_file = dir / filename
    if full_path_to_file.is_file():
        return True
    else:
        raise DbtProfileFileMissing(f"Could not locate `{filename}` in {dir.resolve()}")