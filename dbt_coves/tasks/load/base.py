import glob
import json
import subprocess
from pathlib import Path

from dbt_coves.tasks.base import NonDbtBaseTask
from dbt_coves.utils import shell


class LoadException(Exception):
    pass


class BaseLoadTask(NonDbtBaseTask):
    def __init__(self, args, config):
        super().__init__(args, config)

    def retrieve_all_jsons_from_path(self, path):
        jsons = []

        for file in glob.glob(path + "/*.json"):
            filepath = Path(file)
            if filepath.stat().st_size > 0:
                with open(filepath, "r") as json_file:
                    jsons.append(json.load(json_file))
        return jsons
