# Generate Sources Test

# Imports
import pytest
import os
import subprocess
import shutil
import yaml
from jinja2 import Template
from dotenv import load_dotenv
from glob import glob

# Snowflake
import snowflake.connector

# Bigquery
from google.cloud import bigquery
from google.oauth2 import service_account

# Load env vars
load_dotenv()

# Functions


def get_adapter(project_dir):
    with open(os.path.join(project_dir, "dbt_project.yml"), "r") as f:
        dbt_project = yaml.load(f, Loader=yaml.FullLoader)
    dbt_project = dbt_project["profile"]

    home_dir = os.path.expanduser("~")
    dbt_profiles_path = os.path.join(home_dir, ".dbt", "profiles.yml")
    with open(dbt_profiles_path, "r") as f:
        dbt_profiles = yaml.load(f, Loader=yaml.FullLoader)
    adapter = dbt_profiles[dbt_project]["outputs"]["dev"]["type"]

    return adapter


def get_connector_snowflake(user, password, account, warehouse, role, database):
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        role=role,
        database=database,
    )
    return conn


def get_client_bigquery(sa_key, project_id):
    # Generate SA credentials file
    with open("service_account.json", "w") as f:
        f.write(sa_key)

    # Get BigQuery Client
    credentials = service_account.Credentials.from_service_account_file(
        "service_account.json",
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    client = bigquery.Client(credentials=credentials, project=project_id)

    return client


def get_cases(path_dir):
    case_folders = glob(f"{path_dir}/*", recursive=True)
    cases_list = []
    for folder in case_folders:
        case_dict_input = {}
        case_dict_expected = {}
        case_dict_input["output_dir"] = f"{folder}/output"
        if os.path.isdir(folder):
            case_resources = glob(f"{folder}/*", recursive=True)
            for resource in case_resources:
                if resource == f"{folder}/settings.yml":
                    with open(resource, "r") as f:
                        settings = yaml.load(f, Loader=yaml.FullLoader)
                    case_dict_input["settings"] = settings
                if resource == f"{folder}/input":
                    input_files = glob(f"{resource}/*", recursive=True)
                    for file in input_files:
                        if file == f"{folder}/input/data.sql":
                            case_dict_input["insert_data_sql_file"] = file
                        if file == f"{folder}/input/create_model.sql":
                            case_dict_input["create_model_sql_file"] = file
                        if file == f"{folder}/input/metadata.csv":
                            case_dict_input["metadata_file"] = file
                if resource == f"{folder}/expected":
                    expected_files = glob(f"{resource}/*", recursive=True)
                    for file in expected_files:
                        if file == f"{folder}/expected/data.csv":
                            case_dict_expected["data_csv_file"] = file
                        if file == f"{folder}/expected/source_model.yml":
                            case_dict_expected["source_model"] = file
                        if file == f"{folder}/expected/table_model.yml":
                            case_dict_expected["table_model"] = file
                if resource == f"{folder}/dbt_project.yml" and os.path.isfile(resource):
                    case_dict_input["dbt_project_dir"] = os.path.dirname(resource)
                    case_dict_input["adapter"] = get_adapter(os.path.dirname(resource))
            cases_list.append((case_dict_input, case_dict_expected))
    return cases_list


# Check case folders
cases_list = get_cases("tests/generate_sources_cases")

# Start test cases


@pytest.mark.dependency(name="generate_data")
@pytest.mark.parametrize("input, expected", cases_list)
def test_generate_data(input, expected):

    # Check adapter
    if input["adapter"] == "snowflake":
        # Check env vars
        assert "USER_SNOWFLAKE" in os.environ
        assert "PASSWORD_SNOWFLAKE" in os.environ
        assert "ACCOUNT_SNOWFLAKE" in os.environ
        assert "WAREHOUSE_SNOWFLAKE" in os.environ
        assert "ROLE_SNOWFLAKE" in os.environ
        assert "DATABASE_SNOWFLAKE" in os.environ
        assert "SCHEMA_SNOWFLAKE" in os.environ
        assert "TABLE_SNOWFLAKE" in os.environ

        user = os.environ["USER_SNOWFLAKE"]
        password = os.environ["PASSWORD_SNOWFLAKE"]
        account = os.environ["ACCOUNT_SNOWFLAKE"]
        warehouse = os.environ["WAREHOUSE_SNOWFLAKE"]
        role = os.environ["ROLE_SNOWFLAKE"]
        database = os.environ["DATABASE_SNOWFLAKE"]
        schema = os.environ["SCHEMA_SNOWFLAKE"]
        table = os.environ["TABLE_SNOWFLAKE"]

        # Get connector
        conn = get_connector_snowflake(
            user=user,
            password=password,
            account=account,
            warehouse=warehouse,
            role=role,
            database=database,
        )

        # Generate data
        with conn.cursor() as cursor:
            cursor.execute(f"USE WAREHOUSE {warehouse};")
            cursor.execute(f"USE ROLE {role};")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database};")
            cursor.execute(f"USE DATABASE {database};")
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            cursor.execute(f"USE SCHEMA {schema};")
            with open(input["create_model_sql_file"], "r") as sql_file:
                query = sql_file.read()
            cursor.execute(query)
            with open(input["insert_data_sql_file"], "r") as sql_file:
                query = sql_file.read()
            cursor.execute(query)
            conn.commit()
            cursor.execute(f"SELECT * FROM {schema}.{table} LIMIT 1;")
            result = cursor.fetchall()
        assert len(result) >= 1
    elif input["adapter"] == "redshift":
        pass
    elif input["adapter"] == "bigquery":
        # Check env vars
        assert "PROJECT_DIR_BIGQUERY" in os.environ
        assert "PROJECT_BIGQUERY" in os.environ
        assert "SERVICE_ACCOUNT_GCP" in os.environ
        assert "DATASET_BIGQUERY" in os.environ
        assert "TABLE_BIGQUERY" in os.environ

        # Get env vars
        dataset_id = os.environ["DATASET_BIGQUERY"]
        test_table = os.environ["TABLE_BIGQUERY"]
        project_id = os.environ["PROJECT_BIGQUERY"]
        sa_key = os.environ["SERVICE_ACCOUNT_GCP"]

        # Get client
        client = get_client_bigquery(sa_key, project_id)

        # Generate data
        dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
        dataset.location = "US"
        dataset = client.create_dataset(dataset, timeout=30)
        with open(input["create_model_sql_file"], "r") as sql_file:
            query = sql_file.read()
        query_job = client.query(query, location="US")
        query_job.result()
        assert query_job.errors == None
        with open(input["insert_data_sql_file"], "r") as sql_file:
            query = sql_file.read()
        query_job = client.query(query, location="US")
        query_job.result()
        assert query_job.errors == None
        query_job = client.query(f"SELECT * FROM {dataset_id}.{test_table} LIMIT 1;")
        query_job.result()
        rows = []
        assert query_job.errors == None
        for row in query_job:
            rows.append(row)
        assert len(rows) == 1
    else:
        raise Exception("Adapter not supported")


@pytest.mark.dependency(name="generate_sources", depends=["generate_data"])
@pytest.mark.parametrize("input, expected", cases_list)
def test_generate_sources(input, expected):
    # Generate sources command
    command = [
        "dbt-coves",
        "generate",
        "sources",
        "--metadata",
        os.path.join(os.getcwd(), input["metadata_file"]),
        "--database",
        input["settings"]["database"],
        "--schemas",
        input["settings"]["schema"],
        "--relations",
        input["settings"]["table"],
        "--project-dir",
        input["dbt_project_dir"],
        "--update-strategy",
        "update",
        "--models-destination",
        os.path.join(
            os.getcwd(),
            input["output_dir"],
            "models/staging/{{schema}}/{{relation}}.sql",
        ),
        "--model-props-destination",
        os.path.join(
            os.getcwd(),
            input["output_dir"],
            "models/staging/{{schema}}/{{relation}}.yml",
        ),
        "--sources-destination",
        os.path.join(
            os.getcwd(), input["output_dir"], "models/staging/{{schema}}/sources.yml"
        ),
        "--verbose",
    ]

    # Execute CLI command and interact with it
    process = subprocess.run(
        args=command, input="\n\x1B[B\x1B[B\x1B[B\n", encoding="utf-8"
    )

    assert process.returncode == 0
    assert os.path.isdir(os.path.join(input["output_dir"], "models")) == True


@pytest.mark.dependency(name="check_data", depends=["generate_sources"])
@pytest.mark.parametrize("input, expected", cases_list)
def test_check_models(input, expected):
    # Convert SQL to Jinja2 Template and replace function
    def source(schema, table):
        return f"{schema}.{table}"

    with open(
        os.path.join(
            os.getcwd(),
            input["output_dir"],
            "models",
            "staging",
            input["settings"]["schema"].lower(),
            input["settings"]["table"].lower() + ".sql",
        ),
        "r",
    ) as file:
        table_sql = file.read()

    template_sql = Template(table_sql)
    fields = {"source": source}
    query = template_sql.render(**fields)
    query = query + " LIMIT 1;"

    # Connect and execute query

    if input["adapter"] == "snowflake":
        conn = get_connector_snowflake(
            user=os.environ["USER_SNOWFLAKE"],
            password=os.environ["PASSWORD_SNOWFLAKE"],
            account=os.environ["ACCOUNT_SNOWFLAKE"],
            warehouse=os.environ["WAREHOUSE_SNOWFLAKE"],
            role=os.environ["ROLE_SNOWFLAKE"],
            database=input["settings"]["database"],
        )

        # Execute and validate query
        with conn.cursor() as cursor:
            result = cursor.execute(query).fetch_pandas_all()

        # Save output data to CSV

        result.to_csv(
            os.path.join(os.getcwd(), input["output_dir"], "data.csv"),
            sep=",",
            index=False,
        )
    elif input["adapter"] == "redshift":
        pass
    else:
        raise Exception("Adapter not supported")


    # Read CSV output data
    with open(os.path.join(os.getcwd(), input["output_dir"], "data.csv"), "r") as csv_file:
        output_data = csv_file.readlines()

    # Read CSV expected data
    with open(os.path.join(os.getcwd(), expected["data_csv_file"]), "r") as csv_file:
        expected_data = csv_file.readlines()

    # Diff data

    diff_data = set(output_data).symmetric_difference(set(expected_data))

    assert len(list(diff_data)) == 0

    # Diff models

    # Source model
    with open(
        os.path.join(
            os.getcwd(),
            input["output_dir"],
            "models",
            "staging",
            input["settings"]["schema"].lower(),
            "sources.yml",
        ),
        "r",
    ) as file_1:
        source_model_output = file_1.readlines()

    with open(os.path.join(os.getcwd(), expected["source_model"]), "r") as file_2:
        source_model_expected = file_2.readlines()

    diff_files = set(source_model_output).symmetric_difference(
        set(source_model_expected)
    )

    assert len(list(diff_files)) == 0

    # Table model
    with open(
        os.path.join(
            os.getcwd(),
            input["output_dir"],
            "models",
            "staging",
            input["settings"]["schema"].lower(),
            input["settings"]["table"].lower() + ".yml",
        ),
        "r",
    ) as file_1:
        table_model_output = file_1.readlines()

    with open(os.path.join(os.getcwd(), expected["table_model"]), "r") as file_2:
        table_model_expected = file_2.readlines()

    diff_files = set(table_model_output).symmetric_difference(set(table_model_expected))

    assert len(list(diff_files)) == 0
