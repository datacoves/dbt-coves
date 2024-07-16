import threading
import time

from rich.console import Console
from snowflake.connector import DictCursor

console = Console()


class CloneDB:
    """
    Class to clone a Snowflake database from one to another. This is intended to be used in a blue/green deployment and
    will clone the schemas and grants from the blue database to the green database.
    """

    def __init__(
        self, blue_database: str, green_database: str, snowflake_conn, thread_count: int = 20
    ):
        """
        Blue/Green deployment for Snowflake databases.
        Args:
            blue_database: The current production database.
            green_database: The temporary database where the build will occur.
        """
        self.start_time = time.time()
        self.time_check = self.start_time
        self._list_of_schemas_to_exclude = [
            "INFORMATION_SCHEMA",
            "ACCOUNT_USAGE",
            "SECURITY",
            "SNOWFLAKE",
            "UTILS",
            "PUBLIC",
        ]
        self.blue_database = blue_database
        self.green_database = green_database
        self.con = snowflake_conn
        self._thread_count = thread_count

    def drop_database(self):
        """
        Utility function to drop the green database.

        Returns:
            None
        """
        console.print(f"Dropping database [green]{self.green_database}[/green]")
        self.con.cursor().execute(f"drop database if exists {self.green_database};")

    def create_database(self, database: str):
        """
        Creates the specified database.
        """
        console.print(f"Creating database [green]{self.green_database}[/green]")
        self.con.cursor().execute(f"create database {database};")

    def clone_database_grants(self, blue_database: str, green_database: str):
        """
        Clones the grants from the blue database to the green database.

        Args:
            blue_database: The name of the blue database (prod)
            green_database: The name of the green database (staging).

        Returns:
            None
        """
        console.print(
            f"Cloning grants from [blue]{self.blue_database}[/blue] to green [green]{self.green_database}[/green]"
        )
        dict_cursor = self.con.cursor(DictCursor)
        grants_sql_stg_1 = f"""show grants on database {blue_database}"""
        dict_cursor.execute(grants_sql_stg_1)
        grants = dict_cursor.fetchall()
        threaded_run_commands = ThreadedRunCommands(self.con, self._thread_count)
        for grant in grants:
            grant_sql = (
                f"GRANT {grant['privilege']} ON {grant['granted_on']} {green_database} "
                f"TO ROLE {grant['grantee_name']};"
            )

            threaded_run_commands.register_command(grant_sql)
        threaded_run_commands.run()

    def clone_database_schemas(self, blue_database: str, green_database: str):
        """
        Clones the schemas from the blue database to the green database and clones the existing blue database schema
        grants.

        Args:
            green_database: The name of the green database (staging).
            blue_database: The name of the blue database (prod)

        Returns:
            None
        """
        console.print(
            f"Cloning [u]schemas[/u] from [blue]{self.blue_database}[/blue] to [green]{self.green_database}[/green]"
        )
        dict_cursor = self.con.cursor(DictCursor)
        dict_cursor.execute(f"show schemas in database {blue_database};")
        schemas = dict_cursor.fetchall()
        threaded_run_commands = ThreadedRunCommands(self.con, self._thread_count)

        # Clone schemas
        for schema in schemas:
            if schema["name"] not in self._list_of_schemas_to_exclude:
                # Clone each schema
                sql = f"create schema {green_database}.{schema['name']} clone {blue_database}.{schema['name']};"
                threaded_run_commands.register_command(sql)
        threaded_run_commands.run()
        console.print(f"Cloned schemas in {time.time() - self.time_check} seconds.")
        self.time_check = time.time()
        # Copy grants from Blue DB schemas
        console.print(
            f"Cloning [u]schema grants[/u] from [blue]{self.blue_database}[/blue] to "
            f"[green]{self.green_database}[/green]"
        )
        for schema in schemas:
            if schema["name"] not in self._list_of_schemas_to_exclude:
                grants_sql_stg_1 = f"""show grants on schema {blue_database}.{schema['name']}"""
                dict_cursor.execute(grants_sql_stg_1)
                grants = dict_cursor.fetchall()
                for grant in grants:
                    sql = (
                        f"GRANT {grant['privilege']} ON {grant['granted_on']} {green_database}.{schema['name']}"
                        f"TO ROLE {grant['grantee_name']};"
                    )
                    # Load SQL into the threaded commands to run.
                    threaded_run_commands.register_command(sql)
        threaded_run_commands.run()
        print(f"Cloned grants to schemas in {time.time() - self.time_check} seconds.")
        self.time_check = time.time()


class ThreadedRunCommands:
    """Helper class for running queries across a configurable number of threads"""

    def __init__(self, con, threads):
        self.threads = threads
        self.register_command_thread = 0
        self.thread_commands = [[] for _ in range(self.threads)]
        self.con = con

    def register_command(self, command: str):
        """
        Register a sql command to be run in a thread.

        Args:
            command: A SQL string to be run.

        Returns:
            None
        """
        self.thread_commands[self.register_command_thread].append(command)
        if self.register_command_thread + 1 == self.threads:
            self.register_command_thread = 0
        else:
            self.register_command_thread += 1

    def run_commands(self, commands):
        """
        Loops over the commands passing off to the "run
        Args:
            commands: A list of SQL commands to be run async.

        Returns:
            None
        """
        for command in commands:
            self.con.cursor().execute_async(command)

    def run(self):
        """
        Run the commands in the threads.

        Returns:
            None
        """
        procs = []
        for v in self.thread_commands:
            proc = threading.Thread(target=self.run_commands, args=(v,))
            procs.append(proc)
            proc.start()
        # complete the processes
        for proc in procs:
            proc.join()


# if __name__ == "__main__":
#     '''
#     This section is really only designed for testing purposes. When used in production, it's is intended that you will
#     call the clone_blue_db_to_green method from an external script or directly from the DAG as needed.
#     '''
#     parser = argparse.ArgumentParser(
#         description="Script to run a blue/green swap")

#     # Add the arguments
#     parser.add_argument('--blue-db', type=str, default=os.environ.get('DATACOVES__MAIN__DATABASE'),
#                         help='The source database.')
#     parser.add_argument('--green-db', type=str, help='The name of the green (temporary build) database.')

#     # Parse the arguments
#     args = parser.parse_args()

#     # Handle the case when --green-db is not provided
#     if args.green_db is None:
#         args.green_db = f'{args.blue_db}_STAGING'

#     blue_db = args.blue_db
#     green_db = args.green_db

#     c = CloneDB(blue_db, green_db)
#     c.clone_blue_db_to_green()
