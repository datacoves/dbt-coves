import json
import os
import subprocess
from pathlib import Path

BASEDIR = os.path.abspath(os.path.dirname(__file__))

subprocess.run(["op", "signin"])
onepassword_entry = subprocess.check_output(
    ["op", "item", "get", "dbt-coves-tests", "--format", "json"]
)
onepassword_entry = json.loads(onepassword_entry)
dbt_coves_fields = onepassword_entry.get("fields")
with open(os.path.join(BASEDIR, ".env"), "w+") as f:
    for field in dbt_coves_fields:
        if "value" in field:
            f.write("{}={}\n".format(field["label"], field["value"]))
