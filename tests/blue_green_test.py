"""
Test should:

Create Prod DB and populate with dummy data
    Name comes from dbt-coves blue-green --prod flag
    Run sql table creation and insert
    Grant some usage

Get info from prod db:
    This I have no idea how to, some SHOW GRANTS??

Run dbt-coves blue-green:
    Test has a TEST_STAGING_MODEL model inside, which should end up being the difference between DBs


Asserts:
    Prod has 2 models
    ...
"""

import os
import subprocess
from pathlib import Path

import pytest
import snowflake.connector
from ruamel.yaml import YAML
from snowflake.connector import DictCursor

yaml = YAML()
try:
    FIXTURE_DIR = Path(Path(__file__).parent.absolute(), "blue_green")
    SETTINGS = yaml.load(open(f"{FIXTURE_DIR}/settings.yml"))
    DBT_COVES_SETTINGS = SETTINGS["blue_green"]
except KeyError:
    raise KeyError("blue_green not found in settings")


@pytest.fixture(scope="class")
def snowflake_connection(request):
    # Check env vars
    assert "DATACOVES__DBT_COVES_TEST__USER" in os.environ
    assert "DATACOVES__DBT_COVES_TEST__PASSWORD" in os.environ
    assert "DATACOVES__DBT_COVES_TEST__ACCOUNT" in os.environ
    assert "DATACOVES__DBT_COVES_TEST__WAREHOUSE" in os.environ
    assert "DATACOVES__DBT_COVES_TEST__ROLE" in os.environ

    user = os.environ["DATACOVES__DBT_COVES_TEST__USER"]
    password = os.environ["DATACOVES__DBT_COVES_TEST__PASSWORD"]
    account = os.environ["DATACOVES__DBT_COVES_TEST__ACCOUNT"]
    role = os.environ["DATACOVES__DBT_COVES_TEST__ROLE"]
    warehouse = os.environ["DATACOVES__DBT_COVES_TEST__WAREHOUSE"]
    database = os.environ["DATACOVES__DBT_COVES_TEST__DATABASE"]
    schema = "TESTS_BLUE_GREEN"

    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        role=role,
    )

    request.cls.conn = conn
    request.cls.warehouse = warehouse
    request.cls.schema = schema
    request.cls.production_database = database
    request.cls.staging_database = (
        f"{request.cls.production_database}_{DBT_COVES_SETTINGS.get('staging_suffix', 'staging')}"
    )

    yield conn

    conn.close()


@pytest.mark.usefixtures("snowflake_connection")
class TestBlueGreen:
    """
    Test class for testing the blue-green functionality.
    """

    def test_setup_database(self):
        """
        Fixture to set up the database for testing.
        """
        """
        Fixture to set up the database for testing.
        """
        # input dummy data
        self._generate_dummy_data()
        self.conn.prod_creation_timestamp = self._get_db_creation_timestamp(
            self.production_database
        )
        self.conn.prod_grants = self._get_db_grants(self.production_database)

    def test_dbt_coves_bluegreen(self):
        command = [
            "python",
            "../../dbt_coves/core/main.py",
            "blue-green",
            "--project-dir",
            str(FIXTURE_DIR),
            "--profiles-dir",
            str(FIXTURE_DIR),
            "--production-database",
            self.production_database,
            "--keep-staging-db-on-success",
        ]
        if DBT_COVES_SETTINGS.get("drop_staging_db_at_start"):
            command.append("--drop-staging-db-at-start")
        if DBT_COVES_SETTINGS.get("dbt_selector"):
            command.extend(["--dbt-selector", DBT_COVES_SETTINGS["dbt_selector"]])
        if DBT_COVES_SETTINGS.get("staging_suffix"):
            command.extend(["--staging-suffix", DBT_COVES_SETTINGS["staging_suffix"]])
            self.assert_suffix = True

        # Execute CLI command and interact with it
        process = subprocess.run(
            args=command,
            encoding="utf-8",
            cwd=FIXTURE_DIR,
        )

        assert process.returncode == 0

    def test_blue_green_integrity(self):
        """
        Here prod and staging were already swapped
        self.prod == ex staging
        self.staging == ex prod
        Here we'll assert:
        - self.staging_database exists with it's name
        - Grants match
        - self.prod_timestamp is staging timestamp
        - prod has 2 models
        - staging has 1 model
        """
        assert self._check_staging_existence()
        new_staging_timestamp = self._get_db_creation_timestamp(self.staging_database)
        new_prod_grants = self._get_db_grants(self.production_database)
        assert new_staging_timestamp == self.conn.prod_creation_timestamp
        assert new_prod_grants == self.conn.prod_grants
        cursor = self.conn.cursor(DictCursor)
        cursor.execute(f"SHOW TABLES IN {self.production_database}.{self.schema}")
        prod_tables = cursor.fetchall()
        assert len(prod_tables) == 2
        cursor.execute(f"SHOW TABLES IN {self.staging_database}.{self.schema}")
        staging_tables = cursor.fetchall()
        assert len(staging_tables) == 1
        cursor.close()

    def test_cleanup(self):
        with self.conn.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {self.production_database}")
        self.conn.commit()
        self.conn.close()

    def _generate_dummy_data(self):
        with open(f"{FIXTURE_DIR}/input/dummy_data.sql", "r") as file:
            sql_commands = file.read().split(";")

        cursor = self.conn.cursor()
        cursor.execute(f"USE WAREHOUSE {self.warehouse};")
        for command in sql_commands:
            if command.strip():
                cursor.execute(command)
        cursor.close()

    def _get_db_creation_timestamp(self, database: str):
        # Get a database creation timestamp
        timestamp = None
        cursor = self.conn.cursor(DictCursor)
        cursor.execute("SHOW TERSE DATABASES;")
        databases = cursor.fetchall()
        for db in databases:
            if db["name"] == database.upper():
                timestamp = db["created_on"]
        cursor.close()
        return timestamp

    def _get_db_grants(self, database: str):
        # Get a database creation timestamp and grants
        sql_grants = None
        cursor = self.conn.cursor(DictCursor)
        cursor.execute(f"SHOW GRANTS ON DATABASE {database};")
        sql_grants = cursor.fetchall()
        grants = []
        # remove created_on from grants
        for grant in sql_grants:
            grant.pop("created_on", None)
            grants.append(grant)
        cursor.close()
        return sql_grants

    def _check_staging_existence(self):
        cursor = self.conn.cursor(DictCursor)
        cursor.execute("SHOW TERSE DATABASES;")
        databases = cursor.fetchall()
        for db in databases:
            if db["name"] == self.staging_database.upper():
                return True
        cursor.close()
        return False
