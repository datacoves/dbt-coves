from jinja2 import Template
import os
from dotenv import load_dotenv
from glob import glob
import yaml

# Jinja functions
def env_var(key):
    return os.getenv(key)


# Load ENV

load_dotenv()

# Create profiles.yml

with open(os.path.join("tests", "templates", "profiles.yml.template"), "r") as file:
    template = Template(file.read())
    fields = {"env_var": env_var}

with open(os.path.join("tests", "profiles.yml"), "w") as file:
    file.write(template.render(fields))

# Create dbt_project.yml

with open(os.path.join("tests", "templates", "dbt_project.yml.template"), "r") as file:
    template = Template(file.read())

path_dir = os.path.join("tests", "generate_sources_cases")
case_folders = glob(f"{path_dir}/*", recursive=True)


for folder in case_folders:
    with open(f"{folder}/settings.yml", "r") as f:
        settings = yaml.load(f, Loader=yaml.FullLoader)
    fields = {"profile": settings["profile"]}
    with open(os.path.join(folder, "dbt_project.yml"), "w") as file:
        file.write(template.render(fields))
