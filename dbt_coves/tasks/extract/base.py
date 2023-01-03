import json

from dbt_coves.tasks.base import NonDbtBaseTask


class ExtractException(Exception):
    pass


class BaseExtractTask(NonDbtBaseTask):
    def __init__(self, args, config):
        super().__init__(args, config)

    def save_json(self, path, object):
        try:
            with open(path, "w") as json_file:
                json.dump(object, json_file, indent=4)
        except OSError as e:
            raise ExtractException(f"Couldn't write {path}: {e}")
