import threading
import time

from rich.console import Console

console = Console()

# SHOW PARAMETERS `type` values whose values are written unquoted in ALTER ... SET.
_UNQUOTED_PARAMETER_TYPES = {"BOOLEAN", "NUMBER", "INTEGER"}


def _sql_string(value: str) -> str:
    """Render a Python string as a Snowflake single-quoted string literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _object_type_plural(grant_on: str) -> str:
    """
    Turn a SHOW FUTURE/INHERITED GRANTS object type (e.g. TABLE, DYNAMIC_TABLE,
    PASSWORD_POLICY) into the plural used by GRANT ... ON FUTURE / ON ALL <plural>.
    """
    words = grant_on.replace("_", " ").upper()
    if words.endswith("Y"):
        return words[:-1] + "IES"
    return words + "S"


def _is_true(value) -> bool:
    return str(value).lower() == "true"


class CloneDB:
    """
    Class to clone a Snowflake database from one to another. This is intended to be used in a
    blue/green deployment and will clone the schemas and grants from the blue database to the
    green database.
    """

    def __init__(
        self,
        blue_database: str,
        green_database: str,
        snowflake_conn,
        thread_count: int = 20,
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

    def drop_database(self, if_exists: bool = True):
        """
        Utility function to drop the green database.

        Args:
            if_exists: Use DROP ... IF EXISTS. Pass False when the database is known
                to exist: Snowflake reports a database the role isn't authorized on
                as "does not exist or not authorized", which IF EXISTS would turn
                into a silent no-op.

        Returns:
            None
        """
        console.print(f"Dropping database [green]{self.green_database}[/green]")
        if_exists_sql = "if exists " if if_exists else ""
        self.con.cursor().execute(
            f"drop database {if_exists_sql}{self.green_database};"
        )

    def create_database(self, database: str):
        """
        Creates the specified database.
        """
        console.print(f"Creating database [green]{self.green_database}[/green]")
        self.con.cursor().execute(f"create database {database};")

    def clone_database_properties(self, blue_database: str, green_database: str):
        """
        Copies the parameters explicitly set on the blue database (level DATABASE in
        SHOW PARAMETERS, e.g. DATA_RETENTION_TIME_IN_DAYS) and its comment to the
        green database. The green database is created bare rather than cloned, so
        without this they revert to their defaults after the swap.

        Args:
            blue_database: The name of the blue database (prod)
            green_database: The name of the green database (staging).

        Returns:
            None
        """
        from snowflake.connector import DictCursor

        console.print(
            f"Cloning [u]database parameters[/u] from [blue]{blue_database}[/blue] to "
            f"[green]{green_database}[/green]"
        )
        dict_cursor = self.con.cursor(DictCursor)
        dict_cursor.execute(f"show parameters in database {blue_database};")
        parameters = dict_cursor.fetchall()
        for parameter in parameters:
            if parameter["level"] != "DATABASE":
                continue
            value = parameter["value"]
            if parameter["type"] not in _UNQUOTED_PARAMETER_TYPES:
                value = _sql_string(value)
            self.con.cursor().execute(
                f"alter database {green_database} set {parameter['key']} = {value};"
            )

        # SHOW ... LIKE is case-insensitive and treats _ as a wildcard, so match the
        # exact name afterwards.
        dict_cursor.execute(f"show databases like {_sql_string(blue_database)};")
        for database in dict_cursor.fetchall():
            if database["name"].upper() == blue_database.upper():
                if database.get("comment"):
                    self.con.cursor().execute(
                        f"comment on database {green_database} is "
                        f"{_sql_string(database['comment'])};"
                    )
                break

    def _register_container_grants(
        self,
        threaded_commands: "ThreadedRunCommands",
        container_type: str,
        blue_container: str,
        green_container: str,
    ):
        """
        Registers the statements that recreate on the green container (DATABASE or
        SCHEMA) the future and inherited grants defined on the blue one. Both belong
        to the container object itself, so they are lost when it's swapped out.
        """
        self._register_future_grants(
            threaded_commands, container_type, blue_container, green_container
        )
        self._register_inherited_grants(
            threaded_commands, container_type, blue_container, green_container
        )

    def _register_inherited_grants(
        self,
        threaded_commands: "ThreadedRunCommands",
        container_type: str,
        blue_container: str,
        green_container: str,
    ):
        """
        Registers GRANT INHERITED statements for the inherited grants defined on the
        blue container. SHOW INHERITED GRANTS returns one row per privilege, object
        type and grantee (e.g. SELECT on TABLE to READER).
        """
        from snowflake.connector import DictCursor
        from snowflake.connector.errors import ProgrammingError

        dict_cursor = self.con.cursor(DictCursor)
        try:
            dict_cursor.execute(
                f"show inherited grants in {container_type.lower()} {blue_container};"
            )
        except ProgrammingError as e:
            # Inherited grants need FEATURE_RBAC_INHERITED_GRANTS; accounts without
            # it can't have any to copy.
            console.print(
                f"[yellow]Couldn't list inherited grants in {container_type} "
                f"{blue_container}, skipping them: {e}[/yellow]"
            )
            return
        for grant in dict_cursor.fetchall():
            if grant["inherited_from"] != container_type:
                continue
            if grant["granted_to"] != "ROLE":
                console.print(
                    f"[yellow]Skipping inherited grant of {grant['privilege']} on "
                    f"{grant['granted_on']} to {grant['granted_to']} "
                    f"{grant['grantee_name']}: only grants to roles are copied."
                    "[/yellow]"
                )
                continue
            threaded_commands.register_command(
                f"GRANT INHERITED {grant['privilege']} ON ALL "
                f"{_object_type_plural(grant['granted_on'])} IN "
                f"{container_type} {green_container} "
                f"TO ROLE {grant['grantee_name']};"
            )

    def _register_future_grants(
        self,
        threaded_commands: "ThreadedRunCommands",
        container_type: str,
        blue_container: str,
        green_container: str,
    ):
        """
        Registers GRANT ... ON FUTURE statements that recreate on the green container
        (DATABASE or SCHEMA) the future grants defined on the blue one.
        """
        from snowflake.connector import DictCursor

        dict_cursor = self.con.cursor(DictCursor)
        dict_cursor.execute(
            f"show future grants in {container_type.lower()} {blue_container};"
        )
        for grant in dict_cursor.fetchall():
            if grant["grant_to"] != "ROLE":
                console.print(
                    f"[yellow]Skipping future grant of {grant['privilege']} on "
                    f"{grant['grant_on']} to {grant['grant_to']} "
                    f"{grant['grantee_name']}: only grants to roles are copied."
                    "[/yellow]"
                )
                continue
            grant_option = (
                " WITH GRANT OPTION" if _is_true(grant.get("grant_option")) else ""
            )
            threaded_commands.register_command(
                f"GRANT {grant['privilege']} ON FUTURE "
                f"{_object_type_plural(grant['grant_on'])} IN "
                f"{container_type} {green_container} "
                f"TO ROLE {grant['grantee_name']}{grant_option};"
            )

    @staticmethod
    def _grant_sql(grant: dict, green_object: str) -> str:
        """
        Builds the GRANT statement that recreates a SHOW GRANTS ON row on the green
        object. Ownership transfers copy the grants issued before them, so they must
        run after the other grants.
        """
        sql = (
            f"GRANT {grant['privilege']} ON {grant['granted_on']} {green_object} "
            f"TO ROLE {grant['grantee_name']}"
        )
        if grant["privilege"] == "OWNERSHIP":
            sql += " COPY CURRENT GRANTS"
        return sql + ";"

    def _run_grants(self, grants: list, green_object: str, extra_commands=None):
        """
        Re-issues SHOW GRANTS ON rows against the green object. Non-ownership grants
        (and any extra_commands registrations, such as future grants) run first,
        then ownership transfers, so the current role still owns the object while
        the other grants are issued.
        """
        threaded_commands = ThreadedRunCommands(self.con, self._thread_count)
        ownership_grants = []
        for grant in grants:
            if grant["privilege"] == "OWNERSHIP":
                ownership_grants.append(grant)
            else:
                threaded_commands.register_command(self._grant_sql(grant, green_object))
        if extra_commands:
            extra_commands(threaded_commands)
        threaded_commands.run()

        ownership_commands = ThreadedRunCommands(self.con, self._thread_count)
        for grant in ownership_grants:
            ownership_commands.register_command(self._grant_sql(grant, green_object))
        ownership_commands.run()

    def clone_database_grants(self, blue_database: str, green_database: str):
        """
        Clones the grants and future grants from the blue database to the green
        database.

        Args:
            blue_database: The name of the blue database (prod)
            green_database: The name of the green database (staging).

        Returns:
            None
        """
        from snowflake.connector import DictCursor

        console.print(
            f"Cloning grants from [blue]{self.blue_database}[/blue] to "
            f"green [green]{self.green_database}[/green]"
        )
        dict_cursor = self.con.cursor(DictCursor)
        dict_cursor.execute(f"show grants on database {blue_database}")
        grants = dict_cursor.fetchall()
        self._run_grants(
            grants,
            green_database,
            extra_commands=lambda commands: self._register_container_grants(
                commands, "DATABASE", blue_database, green_database
            ),
        )

    def clone_database_schemas(self, blue_database: str, green_database: str):
        """
        Clones the schemas from the blue database to the green database and clones the existing
        blue database schema grants.

        Args:
            green_database: The name of the green database (staging).
            blue_database: The name of the blue database (prod)

        Returns:
            None
        """
        from snowflake.connector import DictCursor

        console.print(
            f"Cloning [u]schemas[/u] from [blue]{self.blue_database}[/blue] to "
            f"[green]{self.green_database}[/green]"
        )
        dict_cursor = self.con.cursor(DictCursor)
        dict_cursor.execute(f"show schemas in database {blue_database};")
        schemas = dict_cursor.fetchall()
        threaded_schema_commands = ThreadedRunCommands(self.con, self._thread_count)

        schemas = [
            schema
            for schema in schemas
            if schema["name"] not in self._list_of_schemas_to_exclude
        ]

        # Clone schemas
        for schema in schemas:
            sql = (
                f"create schema {green_database}.{schema['name']} "
                f"clone {blue_database}.{schema['name']};"
            )
            threaded_schema_commands.register_command(sql)
        threaded_schema_commands.run()
        console.print(f"Cloned schemas in {time.time() - self.time_check} seconds.")
        self.time_check = time.time()

        # A schema clone isn't guaranteed to keep MANAGED ACCESS, and losing it would
        # let object owners grant access on their own objects. Re-enable it while
        # the current role still owns the cloned schemas (before ownership grants).
        managed_access_commands = ThreadedRunCommands(self.con, self._thread_count)
        for schema in schemas:
            if "MANAGED ACCESS" in (schema.get("options") or "").upper():
                managed_access_commands.register_command(
                    f"alter schema {green_database}.{schema['name']} "
                    "enable managed access;"
                )
        managed_access_commands.run()

        # Copy grants and future grants from Blue DB schemas
        console.print(
            f"Cloning [u]schema grants[/u] from [blue]{self.blue_database}[/blue] to "
            f"[green]{self.green_database}[/green]"
        )
        schema_grants = []
        for schema in schemas:
            dict_cursor.execute(
                f"""show grants on schema {blue_database}.{schema["name"]}"""
            )
            schema_grants.append((schema["name"], dict_cursor.fetchall()))

        threaded_grants_commands = ThreadedRunCommands(self.con, self._thread_count)
        ownership_commands = ThreadedRunCommands(self.con, self._thread_count)
        for schema_name, grants in schema_grants:
            green_schema = f"{green_database}.{schema_name}"
            for grant in grants:
                commands = (
                    ownership_commands
                    if grant["privilege"] == "OWNERSHIP"
                    else threaded_grants_commands
                )
                commands.register_command(self._grant_sql(grant, green_schema))
            self._register_container_grants(
                threaded_grants_commands,
                "SCHEMA",
                f"{blue_database}.{schema_name}",
                green_schema,
            )
        threaded_grants_commands.run()
        ownership_commands.run()
        print(f"Cloned grants to schemas in {time.time() - self.time_check} seconds.")
        self.time_check = time.time()


class ThreadedRunCommands:
    """Helper class for running queries across a configurable number of threads"""

    def __init__(self, con, threads):
        self.threads = threads
        self.register_command_thread = 0
        self.thread_commands = [[] for _ in range(self.threads)]
        self.con = con
        self.pending_queries = []

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
            cur = self.con.cursor()
            cur.execute_async(command)
            self.pending_queries.append(cur.sfqid)

    def wait_for_completion(self):
        """
        Waits for all queries to complete.
        """
        while self.pending_queries:
            self.pending_queries = [
                q
                for q in self.pending_queries
                if self.con.is_still_running(
                    self.con.get_query_status_throw_if_error(q)
                )
            ]
            console.print(
                f"Waiting for {len(self.pending_queries)} queries to complete before continuing."
            )
            time.sleep(1)

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
        self.wait_for_completion()


# if __name__ == "__main__":
#     '''
#     This section is really only designed for testing purposes. When used in production, it is
#     intended that you will call the clone_blue_db_to_green method from an external script or
#     directly from the DAG as needed.
#     '''
#     parser = argparse.ArgumentParser(
#         description="Script to run a blue/green swap")

#     # Add the arguments
#     parser.add_argument('--blue-db', type=str,
#                         default=os.environ.get('DATACOVES__MAIN__DATABASE'),
#                         help='The source database.')
#     parser.add_argument('--green-db', type=str,
#                         help='The name of the green (temporary build) database.')

#     # Parse the arguments
#     args = parser.parse_args()

#     # Handle the case when --green-db is not provided
#     if args.green_db is None:
#         args.green_db = f'{args.blue_db}_STAGING'

#     blue_db = args.blue_db
#     green_db = args.green_db

#     c = CloneDB(blue_db, green_db)
#     c.clone_blue_db_to_green()
