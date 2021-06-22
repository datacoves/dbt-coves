"""Contains yaml related utils which might get used in places."""

import collections
from pathlib import Path
from typing import Any, Dict

import yaml
import yamlloader

from dbt_coves.core.exceptions import YAMLFileEmptyError


def open_yaml(path: Path) -> Dict[str, Any]:
    """Opens a yaml file... Nothing too exciting there.

    Args:
        path (Path): Full filename path pointing to the yaml file we want to open.

    Returns:
        Dict[str, Any]: A python dict containing the content from the yaml file.
    """
    if path.is_file():
        with open(path, "r") as stream:
            yaml_dict = yaml.load(stream, Loader=yamlloader.ordereddict.CSafeLoader)
            if yaml_dict:
                return yaml_dict
            raise YAMLFileEmptyError(f"The following file {path.resolve()} seems empty.")
    raise FileNotFoundError(f"File {path.resolve()} was not found.")


def save_yaml(path: Path, data: Dict[str, Any]) -> None:
    """Saves a YAML content.

    Args:
        path (Path): Full filename path pointing to the yaml file we want to save.
        data (dict[str, Any]): Data to save in the file.
    """
    with open(path, "w") as outfile:
        data_order_dict = collections.OrderedDict(data)
        yaml.dump(data_order_dict, outfile, width=100, Dumper=yamlloader.ordereddict.CDumper)
