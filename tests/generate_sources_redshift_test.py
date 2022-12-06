# Integration tests of dbt-coves generate sources with Redshift adapter

# Imports
import pytest
import os
import subprocess
import shutil
import csv
import yaml
import redshift_connector
from jinja2 import Template
from dotenv import load_dotenv
import pandas as pd 

# Load env vars for test only
load_dotenv()

# Env Vars
metadata_file = os.environ["METADATA_FILE"]
database = os.environ["DATABASE_REDSHIFT"]
schema = os.environ["SCHEMA_REDSHIFT"]
test_table = os.environ["TABLE_REDSHIFT"]
project_dir = os.environ["PROJECT_DIR_REDSHIFT"]

# Get Redshift connection
conn = redshift_connector.connect(
    host=os.environ["HOST_REDSHIFT"],
    database=os.environ["DATABASE_REDSHIFT"],
    user=os.environ["USER_REDSHIFT"],
    password=os.environ["PASSWORD_REDSHIFT"],
)

# Start tests

@pytest.mark.dependency(name="generate_test_model")
def test_generate_test_model():
    os.chdir(os.path.join("tests", project_dir))
    # Generate test table
    with conn.cursor() as cursor:
        cursor.execute(f"CREATE SCHEMA {schema};")
        with open("CREATE_TEST_MODEL.sql", "r") as sql_file:
            query = sql_file.read()
        cursor.execute(query)
        with open("INSERT_TEST_MODEL.sql", "r") as sql_file:
            query = sql_file.read()
        cursor.execute(query)
        conn.commit()
        cursor.execute(f"SELECT * FROM {schema}.{test_table} LIMIT 1;")
        result = cursor.fetchall()
    assert len(result) == 1


@pytest.mark.dependency(name="generate_sources", depends=["generate_test_model"])
def test_generate_sources_redshift():
    # Generate sources command
    command = [
        "dbt-coves",
        "generate",
        "sources",
        "--metadata",
        metadata_file,
        "--database",
        database,
        "--schemas",
        schema,
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

    with open(metadata_file, "r") as file:
        metadata_csv = csv.reader(file, delimiter=",")
        next(metadata_csv)

        schema_yml = None
        test_model_props = None
        test_model_sql = None

        # Check if schema.yml and test_table.yml exists
        for file in os.listdir(f"models/staging/{schema}"):
            if file == f"{schema}.yml":
                with open(f"models/staging/{schema}/{file}", "r") as file:
                    try:
                        schema_yml = yaml.safe_load(file)
                    except yaml.YAMLError as err:
                        print(err)
                        assert False
                continue

            if file == f"{test_table}.yml":
                with open(f"models/staging/{schema}/{file}", "r") as file:
                    try:
                        test_model_props = yaml.safe_load(file)
                    except yaml.YAMLError as err:
                        print(err)
                        assert False
                continue

            if file == f"{test_table}.sql":
                # TODO: Read SQL File
                with open(f"models/staging/{schema}/{file}", "r") as file:
                    test_model_sql = file.read()
                continue

        # Validate test_table.sql
        assert test_model_sql != None

        # Validate schema.yml
        assert schema_yml != None
        assert schema_yml["sources"][0]["name"] == schema
        assert schema_yml["sources"][0]["database"] == database
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
            assert row[0] == database
            assert row[1] == schema
            assert row[2] == test_table
            # Delete underscore from row name
            if row[3][0] == "_":
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
    with conn.cursor() as cursor:
        cursor.execute(query)
        result: pd.DataFrame = cursor.fetch_dataframe()
    
    # Validate columns
    with open(metadata_file, "r") as file:
        metadata_csv = csv.reader(file, delimiter=",")
        next(metadata_csv)
        for row in metadata_csv:
            assert row[3] in list(result.columns)

    # Validate quantity
    assert len(result) == 1


# Finalizers, clean up
@pytest.fixture(scope="module", autouse=True)
def cleanup_redshift(request):
    def return_root():
        # Return to root folder
        os.chdir("..")
        os.chdir("..")
    def delete_folders():
        # Delete models folder if exists
        shutil.rmtree(os.path.join("tests", project_dir, "models"), ignore_errors=True)
    def delete_test_table():
        # Delete test table
        with conn.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {schema}.{test_table};")
            cursor.execute(f"DROP SCHEMA {schema};")
        conn.commit()
        conn.close()

    request.addfinalizer(delete_folders)
    request.addfinalizer(return_root)
    request.addfinalizer(delete_test_table)
