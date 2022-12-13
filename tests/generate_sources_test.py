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

# Redshift
import redshift_connector

# Load env vars
load_dotenv()

# Functions

# Convert SQL to Jinja2 Template and replace function
def source(schema, table):
    return f"{schema}.{table}"


# Get adapter from dbt profile
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


# Connector snowflake
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


# Connector redshift
def get_connector_redshift(host, user, password, database):
    conn = redshift_connector.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        timeout=180
    )
    return conn


# COnnector Bigquery
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


# Get all cases from folder cases
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
            cases_list.append(
                {
                    "id": os.path.basename(folder),
                    "input": case_dict_input,
                    "expected": case_dict_expected,
                }
            )
    return cases_list


# Check case folders
cases_list = get_cases("tests/generate_sources_cases")

# Generate data tests

generate_data_cases = []

for case in cases_list:
    id = case["id"]
    generate_data_cases.append(
        pytest.param(
            case["input"],
            id=case["id"],
            marks=pytest.mark.dependency(name=f"test_generate_data[{id}]", depends=[]),
        )
    )

# Start tests


@pytest.mark.parametrize("input", generate_data_cases)
def test_generate_data(input):

    # Check adapter
    if input["adapter"] == "snowflake":
        # Check env vars
        assert "USER_SNOWFLAKE" in os.environ
        assert "PASSWORD_SNOWFLAKE" in os.environ
        assert "ACCOUNT_SNOWFLAKE" in os.environ
        assert "WAREHOUSE_SNOWFLAKE" in os.environ
        assert "ROLE_SNOWFLAKE" in os.environ

        user = os.environ["USER_SNOWFLAKE"]
        password = os.environ["PASSWORD_SNOWFLAKE"]
        account = os.environ["ACCOUNT_SNOWFLAKE"]
        warehouse = os.environ["WAREHOUSE_SNOWFLAKE"]
        role = os.environ["ROLE_SNOWFLAKE"]

        database = input["settings"]["database"]
        schema = input["settings"]["schema"]
        table = input["settings"]["table"]

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
        conn.close()
    elif input["adapter"] == "redshift":
        # Check env vars
        assert "HOST_REDSHIFT" in os.environ
        assert "USER_REDSHIFT" in os.environ
        assert "PASSWORD_REDSHIFT" in os.environ

        database = input["settings"]["database"]
        schema = input["settings"]["schema"]
        table = input["settings"]["table"]

        # Get connector
        conn = get_connector_redshift(
            host=os.environ["HOST_REDSHIFT"],
            user=os.environ["USER_REDSHIFT"],
            password=os.environ["PASSWORD_REDSHIFT"],
            database=database,
        )

        with conn.cursor() as cursor:
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            with open(input["create_model_sql_file"], "r") as sql_file:
                query = sql_file.read()
            cursor.execute(query)
            with open(input["insert_data_sql_file"], "r") as sql_file:
                query = sql_file.read()
            cursor.execute(query)
            conn.commit()
            cursor.execute(f"SELECT * FROM {schema}.{table} LIMIT 1;")
            result = cursor.fetchall()
        assert len(result) == 1
        conn.close()

    elif input["adapter"] == "bigquery":
        # Check env vars
        assert "PROJECT_BIGQUERY" in os.environ
        assert "SERVICE_ACCOUNT_GCP" in os.environ

        # Get env vars
        project_id = os.environ["PROJECT_BIGQUERY"]
        sa_key = os.environ["SERVICE_ACCOUNT_GCP"]

        database = input["settings"]["database"]
        schema = input["settings"]["schema"]
        table = input["settings"]["table"]

        # Get client
        client = get_client_bigquery(sa_key, project_id)

        # Generate data
        query_job = client.query(
            f"CREATE SCHEMA IF NOT EXISTS `{project_id}.{schema}`;"
        )
        query_job.result()
        assert query_job.errors == None
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
        query_job = client.query(f"SELECT * FROM {schema}.{table} LIMIT 1;")
        query_job.result()
        rows = []
        assert query_job.errors == None
        for row in query_job:
            rows.append(row)
        assert len(rows) == 1
        client.close()
    else:
        raise Exception("Adapter not supported")


# Generate sources cases

cases_list_generate_sources = []

for case in cases_list:
    id = case["id"]
    cases_list_generate_sources.append(
        pytest.param(
            case["input"],
            id=case["id"],
            marks=pytest.mark.dependency(
                name=f"test_generate_sources[{id}]",
                depends=[f"test_generate_data[{id}]"],
            ),
        )
    )


@pytest.mark.parametrize("input", cases_list_generate_sources)
def test_generate_sources(input):
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


# Check models cases

check_models_cases = []

for case in cases_list:
    id = case["id"]
    check_models_cases.append(
        pytest.param(
            case["input"],
            case["expected"],
            id=case["id"],
            marks=pytest.mark.dependency(
                name=f"test_check_models[{id}]",
                depends=[f"test_generate_data[{id}]", f"test_generate_sources[{id}]"],
            ),
        )
    )


@pytest.mark.parametrize("input, expected", check_models_cases)
def test_check_models(input, expected):
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
        conn.close()
    elif input["adapter"] == "redshift":
        conn = get_connector_redshift(
            host=os.environ["HOST_REDSHIFT"],
            user=os.environ["USER_REDSHIFT"],
            password=os.environ["PASSWORD_REDSHIFT"],
            database=input["settings"]["database"],
        )

        with conn.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetch_dataframe()
            result.to_csv(
                os.path.join(os.getcwd(), input["output_dir"], "data.csv"),
                sep=",",
                index=False,
            )
        conn.close()
    elif input["adapter"] == "bigquery":
        project_id = os.environ["PROJECT_BIGQUERY"]
        sa_key = os.environ["SERVICE_ACCOUNT_GCP"]

        client = get_client_bigquery(sa_key, project_id)

        query_job = client.query(query)
        query_job.result()
        query_job.to_dataframe().to_csv(
            os.path.join(os.getcwd(), input["output_dir"], "data.csv"),
            sep=",",
            index=False,
        )
        assert query_job.errors == None
        client.close()
    else:
        raise Exception("Adapter not supported")

    # Read CSV output data
    with open(
        os.path.join(os.getcwd(), input["output_dir"], "data.csv"), "r"
    ) as csv_file:
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


# Cleanup cases

cleanup_cases = []

for case in cases_list:
    cleanup_cases.append(
        pytest.param(
            case["input"],
            id=case["id"],
            marks=pytest.mark.dependency(name=f"tests_cleanup[{id}]", depends=[]),
        )
    )

# Clear tests
@pytest.mark.parametrize("input", cleanup_cases)
def tests_cleanup(input):
    # Delete models folder if exists
    shutil.rmtree(os.path.join(os.getcwd(), input["output_dir"]), ignore_errors=True)

    if input["adapter"] == "snowflake":
        # Delete Snowflake tests database

        database = input["settings"]["database"]

        conn = get_connector_snowflake(
            user=os.environ["USER_SNOWFLAKE"],
            password=os.environ["PASSWORD_SNOWFLAKE"],
            account=os.environ["ACCOUNT_SNOWFLAKE"],
            warehouse=os.environ["WAREHOUSE_SNOWFLAKE"],
            role=os.environ["ROLE_SNOWFLAKE"],
            database=database,
        )

        with conn.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {database};")
        conn.commit()
        conn.close()
    elif input["adapter"] == "redshift":

        schema = input["settings"]["schema"]
        test_table = input["settings"]["table"]
        database = input["settings"]["database"]

        conn = get_connector_redshift(
            host=os.environ["HOST_REDSHIFT"],
            user=os.environ["USER_REDSHIFT"],
            password=os.environ["PASSWORD_REDSHIFT"],
            database=database,
        )

        with conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.{test_table};")
            cursor.execute(f"DROP SCHEMA IF EXISTS {schema};")
        conn.commit()
        conn.close()
    elif input["adapter"] == "bigquery":
        # Delete Bigquery tests schema

        project_id = os.environ["PROJECT_BIGQUERY"]
        sa_key = os.environ["SERVICE_ACCOUNT_GCP"]

        database = input["settings"]["database"]
        schema = input["settings"]["schema"]
        table = input["settings"]["table"]

        client = get_client_bigquery(sa_key, project_id)

        # Delete table
        job_query = client.query(f"DROP TABLE `{schema}.{table}`;")
        job_query.result()
        assert job_query.errors == None

        # Delete dataset
        job_query = client.query(f"DROP SCHEMA `{schema}`;")
        job_query.result()
        assert job_query.errors == None

        client.close()

        # Delete SA
        os.remove("service_account.json")

    else:
        raise Exception("Adapter not supported")
