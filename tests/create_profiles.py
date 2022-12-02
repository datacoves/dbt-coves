from jinja2 import Template
import os
from dotenv import load_dotenv

# Jinja functions
def env_var(key):
    return os.getenv(key)


# Load ENV

load_dotenv()

# Create profiles.yml

with open("tests/profiles.yml.template", "r") as file:
    template = Template(file.read())
    fields = {"env_var": env_var}

with open("tests/profiles.yml", "w") as file:
    file.write(template.render(fields))

# Create dbt_project.yml

profiles_dict = [
    {
        "profile": os.environ["PROFILE_DBT_COVES_REDSHIFT"],
        "project_dir": os.environ["PROJECT_DIR_REDSHIFT"],
    },
    {
        "profile": os.environ["PROFILE_DBT_COVES_SNOWFLAKE"],
        "project_dir": os.environ["PROJECT_DIR_SNOWFLAKE"],
    },
    {
        "profile": os.environ["PROFILE_DBT_COVES_BIGQUERY"],
        "project_dir": os.environ["PROJECT_DIR_BIGQUERY"],
    },
]

with open("tests/dbt_project.yml.template", "r") as file:
    template = Template(file.read())

for item in profiles_dict:
    fields = {"profile": item["profile"]}
    project_dir = item["project_dir"]
    with open(f"tests/{project_dir}/dbt_project.yml", "w") as file:
        file.write(template.render(fields))
