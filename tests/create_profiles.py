# Generation of profiles and dbt projects for testing

# Imports
from jinja2 import Template
import os
import pathlib
from dotenv import load_dotenv
from glob import glob
import yaml

# Jinja functions
def env_var(key):
    return os.getenv(key)

# Path service account key
os.environ["SERVICE_ACCOUNT_GCP_PATH"] = os.path.join(
    os.path.dirname(pathlib.Path(__file__).absolute()),
    "service_account.json",
)

# Load env vars
load_dotenv()

# Create profiles.yml

with open(
    os.path.join(
        os.path.dirname(pathlib.Path(__file__).absolute()),
        "templates",
        "profiles.yml.template",
    ),
    "r",
) as file:
    template = Template(file.read())
    fields = {"env_var": env_var}

with open(
    os.path.join(os.path.dirname(pathlib.Path(__file__).absolute()), "profiles.yml"),
    "w",
) as file:
    file.write(template.render(fields))

# Create dbt_project.yml

with open(
    os.path.join(
        os.path.dirname(pathlib.Path(__file__).absolute()),
        "templates",
        "dbt_project.yml.template",
    ),
    "r",
) as file:
    template = Template(file.read())

path_dir = os.path.join(
    os.path.dirname(pathlib.Path(__file__).absolute()), "generate_sources_cases"
)
case_folders = glob(f"{path_dir}/*", recursive=True)


for folder in case_folders:
    with open(f"{folder}/settings.yml", "r") as f:
        settings = yaml.load(f, Loader=yaml.FullLoader)
    fields = {"profile": settings["profile"]}
    with open(os.path.join(folder, "dbt_project.yml"), "w") as file:
        file.write(template.render(fields))
