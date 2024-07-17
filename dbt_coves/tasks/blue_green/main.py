import os
import subprocess

import snowflake.connector
from rich.console import Console
from rich.text import Text

from dbt_coves.core.exceptions import DbtCovesException
from dbt_coves.tasks.base import NonDbtBaseConfiguredTask
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
            "--service-connection-name",
            type=str,
            help="Snowflake service connection name",
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
            "--drop-staging-db-at-start",
            action="store_true",
            help="Drop staging db at start if it already exists",
        )
        ext_subparser.add_argument(
            "--drop-staging-db-on-failure",
            action="store_true",
            help="Drop staging db if blue-green fails",
        )
        ext_subparser.add_argument(
            "--keep-staging-db-on-success",
            action="store_true",
            help="",
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
        self.service_connection_name = self.get_config_value("service_connection_name").upper()
        try:
            self.production_database = os.environ[
                f"DATACOVES__{self.service_connection_name}__DATABASE"
            ]
        except KeyError:
            raise DbtCovesException(
                f"There is no Database defined for Service Connection {self.service_connection_name}"
            )
        staging_database = self.get_config_value("staging_database")
        staging_suffix = self.get_config_value("staging_suffix")
        if staging_database and staging_suffix:
            raise DbtCovesException("Cannot specify both staging_database and staging_suffix")
        elif not staging_database and not staging_suffix:
            staging_suffix = "STAGING"
        self.staging_database = staging_database or f"{self.production_database}_{staging_suffix}"
        if self.production_database == self.staging_database:
            raise DbtCovesException(
                f"Production database {self.production_database} cannot be the same as staging database "
                f"{self.staging_database}"
            )
        self.drop_staging_db_at_start = self.get_config_value("drop_staging_db_at_start")
        self.con = self.snowflake_connection()

        self.cdb = CloneDB(
            self.production_database,
            self.staging_database,
            self.con,
        )

        self._check_and_drop_staging_db()
        env = os.environ.copy()
        try:
            # create staging db
            self.cdb.create_database(self.staging_database)
            # clones schemas and schema grants from production to pre_production
            self.cdb.clone_database_schemas(self.production_database, self.staging_database)
            # run dbt build
            self._run_dbt_build(env)
            # copy db grants from production db
            self.cdb.clone_database_grants(self.production_database, self.staging_database)
            # Swaps databases: Snowflake sql `alter database {blue} swap with {green}`
            self._swap_databases()
            # drops pre_production (ex production)
            if not self.get_config_value("keep_staging_db_on_success"):
                self.cdb.drop_database()
        except Exception as e:
            if self.get_config_value("drop_staging_db_on_failure"):
                self.cdb.drop_database()
            raise e

        return 0

    def _run_dbt_build(self, env):
        dbt_build_command: list = self._get_dbt_build_command()
        env[f"DATACOVES__{self.service_connection_name}__DATABASE"] = self.staging_database
        self._run_command(dbt_build_command, env=env)

    def _run_command(self, command: list, env=os.environ.copy()):
        command_string = " ".join(command)
        console.print(f"Running [b][i]{command_string}[/i][/b]")
        try:
            output = subprocess.check_output(command, env=env, stderr=subprocess.PIPE)
            console.print(
                f"{Text.from_ansi(output.decode())}\n"
                f"[green]{command_string} :heavy_check_mark:[/green]"
            )
        except subprocess.CalledProcessError as e:
            formatted = f"{Text.from_ansi(e.stderr.decode()) if e.stderr else Text.from_ansi(e.stdout.decode())}"
            e.stderr = f"An error has occurred running [red]{command_string}[/red]:\n{formatted}"
            raise

    def _get_dbt_command(self, command):
        """
        Returns the dbt build command to be run.
        """
        dbt_selector: str = self.get_config_value("dbt_selector")
        is_deferral = self.get_config_value("defer")
        dbt_command = ["dbt", command, "--fail-fast"]
        if is_deferral or os.environ.get("MANIFEST_FOUND"):
            dbt_command.extend(["--defer", "--state", "logs", "-s", "state:modified+"])
        else:
            dbt_command.extend(dbt_selector.split())
        if self.get_config_value("full_refresh"):
            dbt_command.append("--full-refresh")
        if self.args.target:
            dbt_command.extend(["-t", self.args.target])
        return dbt_command

    def _get_dbt_build_command(self):
        return self._get_dbt_command("build")

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
        if green_exists and self.drop_staging_db_at_start:
            self.cdb.drop_database()
        elif green_exists:
            raise DbtCovesException(
                f"Green database {self.staging_database} already exists. Please either drop it or use a different name."
            )

    def snowflake_connection(self):
        try:
            return snowflake.connector.connect(
                account=os.environ.get(f"DATACOVES__{self.service_connection_name}__ACCOUNT"),
                warehouse=os.environ.get(f"DATACOVES__{self.service_connection_name}__WAREHOUSE"),
                database=os.environ.get(f"DATACOVES__{self.service_connection_name}__DATABASE"),
                role=os.environ.get(f"DATACOVES__{self.service_connection_name}__ROLE"),
                schema=os.environ.get(f"DATACOVES__{self.service_connection_name}__SCHEMA"),
                user=os.environ.get(f"DATACOVES__{self.service_connection_name}__USER"),
                password=os.environ.get(f"DATACOVES__{self.service_connection_name}__PASSWORD"),
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
