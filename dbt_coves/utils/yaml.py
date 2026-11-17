"""Contains yaml related utils which might get used in places."""

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

from ruamel.yaml import YAML

yaml = YAML()
yaml.default_flow_style = False
yaml.indent(mapping=2, sequence=4, offset=2)
yaml.preserve_quotes = True
# Don't fold long values: ruamel re-wraps them at 80 characters and leaves a trailing
# space at every fold, so rewriting a properties file reformatted every long
# description and tripped trailing-whitespace hooks.
yaml.width = 4096


def open_yaml(path: Path) -> Dict[str, Any]:
    """Opens a yaml file... Nothing too exciting there.

    Args:
        path (Path): Full filename path pointing to the yaml file we want to open.

    Returns:
        Dict[str, Any]: A python dict containing the content from the yaml file.
    """
    if path.is_file():
        return yaml.load(path)
    raise FileNotFoundError(f"File {path.resolve()} was not found.")


def save_yaml(path: Path, data: Dict[str, Any]) -> None:
    """Saves a YAML content.

    Args:
        path (Path): Full filename path pointing to the yaml file we want to save.
        data (dict[str, Any]): Data to save in the file.
    """
    with open(path, "w") as outfile:
        yaml.dump(data, outfile)


def deep_merge(new_values, default_values):
    """Merge new values into default values dict, overrding existing values"""

    def merge(source, destination):
        for key, value in source.items():
            if isinstance(value, dict):
                # get node or create one
                node = destination.setdefault(key, {})
                merge(value, node)
            else:
                destination[key] = value
        return destination

    default = deepcopy(default_values)
    return merge(new_values, default)
