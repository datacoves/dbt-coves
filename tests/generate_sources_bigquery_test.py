# Integration tests of dbt-coves generate sources with BigQuery adapter

# Imports
import csv
import os
import shutil
import subprocess

import pytest
import yaml
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account
from jinja2 import Template

# Load env vars for test only
load_dotenv()

# Env Vars
metadata_file = os.environ["METADATA_FILE"]
dataset_id = os.environ["DATASET_BIGQUERY"]
test_table = os.environ["TABLE_BIGQUERY"]
project_dir = os.environ["PROJECT_DIR_BIGQUERY"]
project_id = os.environ["PROJECT_BIGQUERY"]

sa_key = os.environ["SERVICE_ACCOUNT_GCP"]

# Generate SA credentials file
with open("service_account.json", "w") as f:
    f.write(sa_key)

sa_key_path = "service_account.json"

# Get BigQuery Client
credentials = service_account.Credentials.from_service_account_file(
    sa_key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

client = bigquery.Client(credentials=credentials, project=os.environ["PROJECT_BIGQUERY"])

# Start tests


@pytest.mark.dependency(name="generate_test_model")
def test_generate_test_model():
    os.chdir(os.path.join("tests", project_dir))
    # Generate dataset
    dataset = bigquery.Dataset(f"{project_id}.{dataset_id}")
    dataset.location = "US"
    dataset = client.create_dataset(dataset, timeout=30)
    # Generate and insert in test model
    with open("CREATE_TEST_MODEL.sql", "r") as sql_file:
        query = sql_file.read()
    query_job = client.query(query, location="US")
    query_job.result()
    assert query_job.errors == None
    with open("INSERT_TEST_MODEL.sql", "r") as sql_file:
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


@pytest.mark.dependency(name="generate_sources", depends=["generate_test_model"])
def test_generate_sources_bigquery():
    # Generate sources command
    command = [
        "dbt-coves",
        "generate",
        "sources",
        "--metadata",
        metadata_file,
        "--schemas",
        dataset_id,
        # "--project-dir",
        # project_dir,
        "--update-strategy",
        "update",
        "--verbose",
    ]

    # Execute CLI command and interact with it
    process = subprocess.run(args=command, input="\n", encoding="utf-8")

    assert process.returncode == 0
    assert os.path.isdir("models") == True

    with open(metadata_file, "r") as file:
        metadata_csv = csv.reader(file, delimiter=",")
        next(metadata_csv)

        schema_yml = None
        test_model_props = None
        test_model_sql = None

        dataset_folder = dataset_id.lower()

        # Check if schema.yml and test_table.yml exists
        for file in os.listdir(f"models/staging/{dataset_folder}"):
            if file == f"{dataset_folder}.yml":
                with open(f"models/staging/{dataset_folder}/{file}", "r") as file:
                    try:
                        schema_yml = yaml.safe_load(file)
                    except yaml.YAMLError as err:
                        print(err)
                        assert False
                continue

            if file == f"{test_table}.yml":
                with open(f"models/staging/{dataset_folder}/{file}", "r") as file:
                    try:
                        test_model_props = yaml.safe_load(file)
                    except yaml.YAMLError as err:
                        print(err)
                        assert False
                continue

            if file == f"{test_table}.sql":
                # TODO: Read SQL File
                with open(f"models/staging/{dataset_folder}/{file}", "r") as file:
                    test_model_sql = file.read()
                continue

        # Validate test_table.sql
        assert test_model_sql != None

        # Validate schema.yml
        assert schema_yml != None
        assert schema_yml["sources"][0]["name"] == dataset_id.lower()

        found_table = False
        for tables in schema_yml["sources"][0]["tables"]:
            if tables["name"] == test_table:
                found_table = True
                break
        assert found_table

        # Validate test_table.yml with metadata.csv
        assert test_model_props != None
        assert test_model_props["models"][0]["name"] == test_table
        for row in metadata_csv:
            # "database","schema","relation","column","key","type","description"
            assert row[0] == project_id
            assert row[1] == dataset_id
            assert row[2] == test_table
            # Delete underscore and get row name
            # Validate if is a key (JSON Flatten)
            if row[4] != "":
                row_name = row[4]
            elif row[3][0] == "_":
                row_name = row[3][1:]
            else:
                row_name = row[3]
            found_col = False
            for col in test_model_props["models"][0]["columns"]:
                if col["name"] == row_name:
                    found_col = True
                    assert col["description"] == row[6]
                    break
            assert found_col

    # Convert SQL to Jinja2 Template and replace function
    def source(schema, table):
        return f"{schema}.{table}"

    template_sql = Template(test_model_sql)
    fields = {"source": source}
    query = template_sql.render(**fields)
    query = query + " LIMIT 1;"

    # Execute and validate query
    query_job = client.query(query)
    query_job.result()
    assert query_job.errors == None

    rows = []

    for row in query_job:
        rows.append(dict(row))

    # Validate quantity
    assert len(rows) == 1

    columns = list(rows[0].keys())

    # Validate columns
    with open(metadata_file, "r") as file:
        metadata_csv = csv.reader(file, delimiter=",")
        next(metadata_csv)
        for row in metadata_csv:
            if row[4] != "":
                row_name = row[4]
            else:
                row_name = row[3]
            assert row_name in list(columns)


# Finalizers, clean up
@pytest.fixture(scope="module", autouse=True)
def cleanup_bigquery(request):
    def delete_sa():
        # Return to root folder
        os.chdir("..")
        os.chdir("..")
        # Delete credentials after use
        os.remove(sa_key_path)

    def delete_folders():
        # Delete models folder if exists
        shutil.rmtree(os.path.join("tests", project_dir, "models"), ignore_errors=True)

    def delete_test_model():
        # Delete test model
        job_query = client.query(f"DROP TABLE {dataset_id}.{test_table};")
        job_query.result()
        assert job_query.errors == None
        # Delete dataset
        job_query = client.query(f"DROP SCHEMA `{project_id}.{dataset_id}`;")
        job_query.result()
        assert job_query.errors == None

    request.addfinalizer(delete_folders)
    request.addfinalizer(delete_sa)
    request.addfinalizer(delete_test_model)
