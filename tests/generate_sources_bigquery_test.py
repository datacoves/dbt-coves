# Integration tests of dbt-coves generate sources with BigQuery adapter

# Imports
import pytest
import os
import subprocess
import shutil
import csv
import yaml
from jinja2 import Template
from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

# Load env vars for test only
load_dotenv()

# Env Vars
metadata_file = os.environ["METADATA_FILE"]
dataset_id = os.environ["DATASET_BIGQUERY"]
test_table = os.environ["TABLE_BIGQUERY"]
project_dir = os.environ["PROJECT_DIR_BIGQUERY"]
project_id = os.environ["PROJECT_BIGQUERY"]

sa_key_path = os.environ["SERVICE_ACCOUNT_GCP"]

# Get BigQuery Client
credentials = service_account.Credentials.from_service_account_file(
    sa_key_path, 
    scopes=["https://www.googleapis.com/auth/cloud-platform"])

client = bigquery.Client(
    credentials=credentials, 
    project=os.environ["PROJECT_BIGQUERY"])

os.chdir(project_dir)

# Start tests

@pytest.mark.dependency(name="generate_test_model")
def test_generate_test_model():
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
        print(row)
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
        # "--relations",
        # test_table,
        # "--project-dir",
        # project_dir,
        "--update-strategy",
        "update",
        "--verbose",
    ]
    print(" ".join(command))
    # Execute CLI command and interact with it
    process = subprocess.run(args=command, input="\n", encoding="utf-8")

    assert process.returncode == 0
    assert os.path.isdir("models") == True

    #with open(metadata_file, "r") as file:
    #    metadata_csv = csv.reader(file, delimiter=",")
    #    next(metadata_csv)

    #    schema_yml = None
    #    test_model_props = None
    #    test_model_sql = None

    #    # Check if schema.yml and test_table.yml exists
    #    for file in os.listdir(f"models/staging/{schema}"):
    #        if file == f"{schema}.yml":
    #            with open(f"models/staging/{schema}/{file}", "r") as file:
    #                try:
    #                    schema_yml = yaml.safe_load(file)
    #                except yaml.YAMLError as err:
    #                    print(err)
    #                    assert False
    #            continue

    #        if file == f"{test_table}.yml":
    #            with open(f"models/staging/{schema}/{file}", "r") as file:
    #                try:
    #                    test_model_props = yaml.safe_load(file)
    #                except yaml.YAMLError as err:
    #                    print(err)
    #                    assert False
    #            continue

    #        if file == f"{test_table}.sql":
    #            # TODO: Read SQL File
    #            with open(f"models/staging/{schema}/{file}", "r") as file:
    #                test_model_sql = file.read()
    #            continue

    #    # Validate test_table.sql
    #    assert test_model_sql != None

    #    # Validate schema.yml
    #    assert schema_yml != None
    #    assert schema_yml["sources"][0]["name"] == schema
    #    assert schema_yml["sources"][0]["database"] == database
    #    found_table = False
    #    for tables in schema_yml["sources"][0]["tables"]:
    #        if tables["name"] == test_table:
    #            found_table = True
    #            break
    #    assert found_table

    #    # Validate test_table.yml with metadata.csv
    #    assert test_model_props != None
    #    assert test_model_props["models"][0]["name"] == test_table
    #    for row in metadata_csv:
    #        # "database","schema","relation","column","key","type","description"
    #        assert row[0] == database
    #        assert row[1] == schema
    #        assert row[2] == test_table
    #        # Delete underscore and get row name
    #        # Validate if is a key (JSON Flatten)
    #        if row[4] != "":
    #            row_name = row[4]
    #        elif row[3][0] == "_":
    #            row_name = row[3][1:]
    #        else:
    #            row_name = row[3]
    #        found_col = False
    #        for col in test_model_props["models"][0]["columns"]:
    #            if col["name"] == row_name:
    #                found_col = True
    #                assert col["description"] == row[6]
    #                break
    #        assert found_col

    ## Convert SQL to Jinja2 Template and replace function
    #def source(schema, table):
    #    return f"{schema}.{table}"

    #template_sql = Template(test_model_sql)
    #fields = {"source": source}
    #query = template_sql.render(**fields)
    #query = query + " LIMIT 1;"

    ## Execute and validate query
    #with conn.cursor() as cursor:
    #    cursor.execute(query)
    #    result = cursor.fetch_pandas_all()
    #
    ## Validate columns
    #with open(metadata_file, "r") as file:
    #    metadata_csv = csv.reader(file, delimiter=",")
    #    next(metadata_csv)
    #    for row in metadata_csv:
    #        if row[4] != "":
    #            row_name = row[4]
    #        else: 
    #            row_name = row[3]
    #        assert row_name.upper() in list(result.columns)

    ## Validate quantity
    #assert len(result) == 1


# Finalizers, clean up
@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    def delete_folders():
        # Delete models folder if exists
        shutil.rmtree("models", ignore_errors=True)
    def delete_test_model():
        # Delete test model
        job_query = client.query(f"DROP TABLE {dataset_id}.{test_table}")
        job_query.result()
        assert job_query.errors == None
        # Delete dataset
        job_query = client.query(f"DROP SCHEMA `{project_id}.{dataset_id}`;")
        job_query.result()
        assert job_query.errors == None
    
    request.addfinalizer(delete_folders)
    request.addfinalizer(delete_test_model)
