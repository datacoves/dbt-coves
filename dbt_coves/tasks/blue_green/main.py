import os
import time

import snowflake.connector
from rich.console import Console

from dbt_coves.core.exceptions import DbtCovesException
from dbt_coves.tasks.base import NonDbtBaseConfiguredTask
from dbt_coves.tasks.dbt.main import RunDbtTask
from dbt_coves.utils.tracking import trackable

from .clone_db import CloneDB

console = Console()


class BlueGreenTask(NonDbtBaseConfiguredTask):
    """
    Task that performs a blue-green deployment
    """

    arg_parser = None

    @classmethod
    def register_parser(cls, sub_parsers, base_subparser):
        ext_subparser = sub_parsers.add_parser(
            "blue-green",
            parents=[base_subparser],
            # help="Run dbt on an isolated environment",
            help="""Command to perfrom blue-green dbt runs""",
        )
        ext_subparser.set_defaults(cls=cls, which="blue-green")
        cls.arg_parser = ext_subparser
        ext_subparser.add_argument(
            "--production-database",
            type=str,
            help="Blue database",
        )
        ext_subparser.add_argument(
            "--staging-database",
            type=str,
            help="Green database",
        )
        ext_subparser.add_argument(
            "--staging-suffix",
            type=str,
            help="Green database suffix",
        )
        ext_subparser.add_argument(
            "--drop-staging-db",
            action="store_true",
            help="Drop staging db after swap",
        )
        ext_subparser.add_argument(
            "--drop-staging-db-after",
            type=int,
            help="Drop staging db after X minutes",
        )
        ext_subparser.add_argument(
            "--drop-staging-db-on-failure",
            action="store_true",
            help="Drop staging db if blue-green fails",
        )
        ext_subparser.add_argument(
            "--dbt-selector",
            type=str,
            help="dbt selector(s) to be passed to build operation",
        )
        ext_subparser.add_argument(
            "--full-refresh",
            action="store_true",
            help="Perform a full dbt build",
        )
        ext_subparser.add_argument("--defer", action="store_true", help="Run in deferral")
        return ext_subparser

    def get_config_value(self, key):
        return self.coves_config.integrated["blue_green"][key]

    @trackable
    def run(self) -> int:
        """
        - deployment_settings:
            drop_staging_db: true (default false)
            drop_pre_prodcution_db_after: 30 (default is 0)
            production_db_name: balboa (or what is in MAIN service cred)
            pre_production_db_suffix: _staging
        """
        production_database = self.get_config_value("production_database")
        staging_database = self.get_config_value("staging_database")
        staging_suffix = self.get_config_value("staging_suffix")
        if staging_database and staging_suffix:
            raise DbtCovesException("Cannot specify both staging_database and staging_suffix")
        elif not staging_database and not staging_suffix:
            staging_suffix = "STAGING"
        self.production_database = production_database or os.environ.get(
            "DATACOVES__MAIN__DATABASE"
        )
        self.staging_database = staging_database or f"{self.production_database}_{staging_suffix}"
        if self.production_database == self.staging_database:
            raise DbtCovesException(
                f"Production database {self.production_database} cannot be the same as staging database "
                f"{self.staging_database}"
            )
        self.drop_staging_db = self.get_config_value("drop_staging_db")
        self.drop_staging_db_after = self.get_config_value("drop_staging_db_after")
        self.con = self.snowflake_connection()

        self.cdb = CloneDB(
            self.production_database,
            self.staging_database,
            self.con,
        )
        self._check_and_drop_staging_db()

        try:
            # create staging db
            self.cdb.create_database(self.staging_database)
            # clones schemas and schema grants from production to pre_production
            self.cdb.clone_database_schemas(self.production_database, self.staging_database)
            # run dbt build
            dbt_build_command: list = self.get_dbt_build_command()
            console.print("Running dbt build")
            RunDbtTask(self.args, self.coves_config).run_dbt(
                command=dbt_build_command, project_dir=self.args.project_dir or None
            )
            # copy db grants from production db
            self.cdb.clone_database_grants(self.production_database, self.staging_database)
            # Swaps databases: Snowflake sql `alter database {blue} swap with {green}`
            self._swap_databases()
            # drops pre_production (ex production)
            self.cdb.drop_database()
        except Exception as e:
            if self.get_config_value("drop_staging_db_on_failure"):
                self.cdb.drop_database()
            raise e

        return 0

    def get_dbt_build_command(self):
        """
        Returns the dbt build command to be run.
        """
        dbt_selector = self.get_config_value("dbt_selector") or []  # []
        is_deferral = self.get_config_value("defer")
        dbt_build_command = ["build", "--fail-fast"]
        if is_deferral:
            dbt_build_command.extend(["--defer", "--state", "logs", "-s", "state:modified+"])
        else:
            dbt_build_command.extend(dbt_selector)
        if self.get_config_value("full_refresh"):
            dbt_build_command.append("--full-refresh")
        if self.args.target:
            dbt_build_command.extend(["-t", self.args.target])
        return dbt_build_command

    def _swap_databases(self):
        """
        Swaps databases: Snowflake sql `alter database {blue} swap with {green}`
        """
        console.print("Swapping databases")
        try:
            sql = f"alter database {self.production_database} swap with {self.staging_database};"
            self.con.cursor().execute(sql)
        except Exception as e:
            print(f"Error swapping databases: {e}")
            raise e

    def _check_and_drop_staging_db(self):
        """
        Checks if the staging database exists and drops it if it does.

        Returns:
            None
        """
        green_exists = self._check_if_database_exists()
        if green_exists and self.drop_staging_db:
            if self.drop_staging_db_after:
                console.print(
                    f"Green database {self.staging_database} exists."
                    f"Waiting {self.drop_staging_db_after} minutes before dropping it"
                )
                for i in range(self.drop_staging_db_after):
                    console.print(f"Waiting {i} minutes")
                    time.sleep(60)
                    green_exists = self._check_if_database_exists()
                    if not green_exists:
                        break
                    if green_exists and i == self.drop_staging_db_after - 1:
                        raise DbtCovesException(
                            f"Green database {self.staging_database} still exists"
                            f"after {self.drop_staging_db_after} minutes"
                        )
            print(f"Dropping database {self.staging_database}.")
            self.cdb.drop_database()
            green_exists = False
        elif green_exists:
            raise DbtCovesException(
                f"Green database {self.staging_database} already exists. Please either drop it or use a different name."
            )

    def snowflake_connection(self):
        try:
            return snowflake.connector.connect(
                account=os.environ.get(f"DATACOVES__{self.production_database}__ACCOUNT"),
                warehouse=os.environ.get(f"DATACOVES__{self.production_database}__WAREHOUSE"),
                database=os.environ.get(f"DATACOVES__{self.production_database}__DATABASE"),
                role=os.environ.get(f"DATACOVES__{self.production_database}__ROLE"),
                schema=os.environ.get(f"DATACOVES__{self.production_database}__SCHEMA"),
                user=os.environ.get(f"DATACOVES__{self.production_database}__USER"),
                password=os.environ.get(f"DATACOVES__{self.production_database}__PASSWORD"),
                session_parameters={
                    "QUERY_TAG": "blue_green_swap",
                },
            )
        except Exception as e:
            raise DbtCovesException(
                f"Couldn't establish Snowflake connection with {self.production_database}: {e}"
            )

    def _check_if_database_exists(self):
        """
        Check if the green database exists and fail if it does

        Returns:
            None
        """
        query = f"SHOW DATABASES LIKE '{self.staging_database}'"
        cursor = self.con.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        if result is not None:
            return True
        else:
            return False
