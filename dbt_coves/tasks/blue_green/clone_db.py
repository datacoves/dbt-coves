import threading
import time
from dataclasses import dataclass, field

from rich.console import Console

from dbt_coves.core.exceptions import DbtCovesException

console = Console()

# SHOW PARAMETERS `type` values whose values are written unquoted in ALTER ... SET.
_UNQUOTED_PARAMETER_TYPES = {"BOOLEAN", "NUMBER", "INTEGER", "FLOAT", "DOUBLE"}


class UnsupportedGrantError(DbtCovesException):
    """Production has grants blue-green can't recreate on the staging database."""


@dataclass
class PostBuildGrants:
    """
    Statements the pre-build steps hold back until after `dbt build`; returned by
    clone_database_schemas and clone_database_container_grants and passed to
    clone_database_grants.
    """

    future_ownership: list = field(default_factory=list)
    managed_access: list = field(default_factory=list)
    schema_ownership: list = field(default_factory=list)

    def __add__(self, other: "PostBuildGrants") -> "PostBuildGrants":
        return PostBuildGrants(
            self.future_ownership + other.future_ownership,
            self.managed_access + other.managed_access,
            self.schema_ownership + other.schema_ownership,
        )


def _sql_string(value: str) -> str:
    """Render a Python string as a Snowflake single-quoted string literal."""
    return "'" + value.replace("\\", "\\\\").replace("'", "''") + "'"


def _quote_identifier(name: str) -> str:
    """
    Quote a name taken from SHOW output (a role or schema), which is the exact
    stored name: quoting keeps lowercase or special characters (e.g. "Data-Eng")
    intact. Names from the user's config are not quoted, as Snowflake resolves
    those case-insensitively.
    """
    return '"' + name.replace('"', '""') + '"'


def find_database(con, name: str):
    """
    Returns the SHOW DATABASES row for `name`, or None. `name` is an unquoted
    identifier from the user's config, which Snowflake resolves to uppercase.
    SHOW ... LIKE is case-insensitive and treats _ as a wildcard (PRODXSTAGING and
    a quoted "prod_staging" both match PROD_STAGING), so the exact uppercase name is
    matched afterwards.
    """
    from snowflake.connector import DictCursor

    cursor = con.cursor(DictCursor)
    cursor.execute(f"show databases like {_sql_string(name)};")
    for database in cursor.fetchall():
        if database["name"] == name.upper():
            return database
    return None


def _object_type_plural(grant_on: str) -> str:
    """
    Turn a SHOW FUTURE/INHERITED GRANTS object type (e.g. TABLE, DYNAMIC_TABLE,
    PASSWORD_POLICY) into the plural used by GRANT ... ON FUTURE / ON ALL <plural>.
    """
    words = grant_on.replace("_", " ").upper()
    if words.endswith("Y") and len(words) > 1 and words[-2] not in "AEIOU":
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

        A parameter the deploy role can't set (e.g. LOG_LEVEL needs MODIFY LOG
        LEVEL on the account) is skipped with a warning rather than failing the
        deploy; it then reverts to its default, as it did before this was copied.

        Args:
            blue_database: The name of the blue database (prod)
            green_database: The name of the green database (staging).

        Returns:
            None
        """
        from snowflake.connector import DictCursor
        from snowflake.connector.errors import ProgrammingError

        console.print(
            f"Cloning [u]database parameters[/u] from [blue]{blue_database}[/blue] to "
            f"[green]{green_database}[/green]"
        )
        dict_cursor = self.con.cursor(DictCursor)
        dict_cursor.execute(f"show parameters in database {blue_database};")
        for parameter in dict_cursor.fetchall():
            if parameter["level"] != "DATABASE":
                continue
            value = parameter["value"]
            if parameter["type"] not in _UNQUOTED_PARAMETER_TYPES:
                value = _sql_string(value)
            try:
                self.con.cursor().execute(
                    f"alter database {green_database} set {parameter['key']} = {value};"
                )
            except ProgrammingError as e:
                console.print(
                    f"[yellow]Couldn't set {parameter['key']} = {value} on "
                    f"{green_database}, it will revert to its default after the swap: "
                    f"{e}[/yellow]"
                )

        database = find_database(self.con, blue_database)
        if database and database.get("comment"):
            self.con.cursor().execute(
                f"comment on database {green_database} is "
                f"{_sql_string(database['comment'])};"
            )

    # ------------------------------------------------------------------------------
    # Grants
    #
    # Grants are copied in two phases around `dbt build`:
    #
    # Before the build (clone_database_schemas, clone_database_container_grants):
    #   grants on schemas, and future grants on the database and schemas, except
    #   OWNERSHIP; inherited grants on the database. Future grants only apply to
    #   objects created after them, so they must exist before dbt creates objects.
    #   The deploy role keeps owning the staging schemas during the build. These
    #   steps return the statements held back for later as PostBuildGrants.
    #
    # After the build (clone_database_grants):
    #   grants on the database except OWNERSHIP, then future OWNERSHIP grants (so
    #   objects dbt created stay owned by the deploy role for the rest of the
    #   build), then MANAGED ACCESS (needs schema ownership, and would otherwise
    #   stop the deploy role granting on its own objects during the build), then
    #   ownership transfers: schemas, then the database.
    # ------------------------------------------------------------------------------

    @staticmethod
    def _grant_sqls(grants: list, green_object: str):
        """
        Builds the GRANT statements that recreate SHOW GRANTS ON rows on the green
        object. Returns (grants, ownership_grants, unsupported): ownership transfers
        copy the grants issued before them, so they're kept separate to run last;
        `unsupported` lists rows whose grantee isn't an account role (a database
        role, share, ...), which can't be recreated on the green database.
        """
        sqls, ownership_sqls, unsupported = [], [], []
        for grant in grants:
            if grant["granted_to"] != "ROLE":
                unsupported.append(
                    f"{grant['privilege']} on {grant['granted_on']} {grant['name']} "
                    f"to {grant['granted_to']} {grant['grantee_name']}"
                )
                continue
            sql = (
                f"GRANT {grant['privilege']} ON {grant['granted_on']} {green_object} "
                f"TO ROLE {_quote_identifier(grant['grantee_name'])}"
            )
            if grant["privilege"] == "OWNERSHIP":
                ownership_sqls.append(sql + " COPY CURRENT GRANTS;")
                continue
            if _is_true(grant.get("grant_option")):
                sql += " WITH GRANT OPTION"
            sqls.append(sql + ";")
        return sqls, ownership_sqls, unsupported

    @staticmethod
    def _raise_unsupported(unsupported: list):
        if unsupported:
            listed = "\n".join(f"  - {grant}" for grant in unsupported)
            raise UnsupportedGrantError(
                "Production has grants blue-green can't recreate on the staging "
                "database, so swapping would silently drop them:\n"
                f"{listed}\n"
                "Only grants to account roles can be copied."
            )

    @staticmethod
    def _warn_skipped(privilege, on, grantee_type, grantee_name):
        console.print(
            f"[yellow]Skipping grant of {privilege} on {on} to {grantee_type} "
            f"{grantee_name}: only grants to account roles are copied.[/yellow]"
        )

    def _future_grant_sqls(
        self, container_type: str, blue_container: str, green_container: str
    ):
        """
        Returns (grants, ownership_grants): GRANT ... ON FUTURE statements that
        recreate on the green container (DATABASE or SCHEMA) the future grants
        defined on the blue one, with future OWNERSHIP grants kept separate.
        """
        from snowflake.connector import DictCursor

        dict_cursor = self.con.cursor(DictCursor)
        dict_cursor.execute(
            f"show future grants in {container_type.lower()} {blue_container};"
        )
        grants, ownership_grants = [], []
        for grant in dict_cursor.fetchall():
            object_plural = _object_type_plural(grant["grant_on"])
            if grant["grant_to"] != "ROLE":
                self._warn_skipped(
                    grant["privilege"],
                    f"future {object_plural} in {container_type} {green_container}",
                    grant["grant_to"],
                    grant["grantee_name"],
                )
                continue
            # OWNERSHIP rows report grant_option=true, but GRANT OWNERSHIP doesn't
            # accept WITH GRANT OPTION.
            grant_option = (
                " WITH GRANT OPTION"
                if grant["privilege"] != "OWNERSHIP"
                and _is_true(grant.get("grant_option"))
                else ""
            )
            sql = (
                f"GRANT {grant['privilege']} ON FUTURE {object_plural} IN "
                f"{container_type} {green_container} "
                f"TO ROLE {_quote_identifier(grant['grantee_name'])}{grant_option};"
            )
            if grant["privilege"] == "OWNERSHIP":
                ownership_grants.append(sql)
            else:
                grants.append(sql)
        return grants, ownership_grants

    def _inherited_grants_enabled(self) -> bool:
        from snowflake.connector import DictCursor

        dict_cursor = self.con.cursor(DictCursor)
        dict_cursor.execute(
            "show parameters like 'FEATURE_RBAC_INHERITED_GRANTS' in account;"
        )
        return any(row["value"] == "ENABLED" for row in dict_cursor.fetchall())

    def _inherited_grant_sqls(self, blue_database: str, green_database: str):
        """
        Returns GRANT INHERITED statements recreating the inherited grants defined
        on the blue database. SHOW INHERITED GRANTS returns one row per privilege,
        object type and grantee (e.g. SELECT on TABLE to READER). Schema-level
        inherited grants aren't needed here: a schema clone keeps them.
        """
        from snowflake.connector import DictCursor
        from snowflake.connector.errors import ProgrammingError

        dict_cursor = self.con.cursor(DictCursor)
        try:
            dict_cursor.execute(f"show inherited grants in database {blue_database};")
        except ProgrammingError as error:
            # Accounts without FEATURE_RBAC_INHERITED_GRANTS can't run this and
            # can't have inherited grants to copy. Anything else (e.g. missing
            # privileges) would silently drop them, so it fails the deploy, with
            # the original error if the feature check can't run either.
            try:
                enabled = self._inherited_grants_enabled()
            except ProgrammingError:
                raise error
            if enabled:
                raise
            console.print(
                "Inherited grants aren't enabled on this account, skipping them."
            )
            return []
        sqls = []
        for grant in dict_cursor.fetchall():
            if grant["inherited_from"] != "DATABASE":
                continue
            object_plural = _object_type_plural(grant["granted_on"])
            if grant["granted_to"] != "ROLE":
                self._warn_skipped(
                    f"inherited {grant['privilege']}",
                    f"all {object_plural} in database {green_database}",
                    grant["granted_to"],
                    grant["grantee_name"],
                )
                continue
            sqls.append(
                f"GRANT INHERITED {grant['privilege']} ON ALL {object_plural} IN "
                f"DATABASE {green_database} "
                f"TO ROLE {_quote_identifier(grant['grantee_name'])};"
            )
        return sqls

    def _show_database_grants(self, blue_database: str) -> list:
        from snowflake.connector import DictCursor

        dict_cursor = self.con.cursor(DictCursor)
        dict_cursor.execute(f"show grants on database {blue_database}")
        return dict_cursor.fetchall()

    def _run_commands(self, commands: list):
        """Runs SQL statements across the thread pool and waits for them."""
        threaded_commands = ThreadedRunCommands(self.con, self._thread_count)
        for command in commands:
            threaded_commands.register_command(command)
        threaded_commands.run()

    def clone_database_schemas(
        self, blue_database: str, green_database: str
    ) -> PostBuildGrants:
        """
        Clones the schemas from the blue database to the green database, then copies
        their grants and future grants except OWNERSHIP.

        Args:
            green_database: The name of the green database (staging).
            blue_database: The name of the blue database (prod)

        Returns:
            The schema OWNERSHIP grants, future OWNERSHIP grants and MANAGED ACCESS
            statements to apply after the build (see clone_database_grants).

        Raises:
            UnsupportedGrantError: the database or a schema has grants to a
                database role, share, etc., which can't be recreated. Raised before
                any schema is cloned.
        """
        from snowflake.connector import DictCursor

        console.print(
            f"Cloning [u]schemas[/u] from [blue]{self.blue_database}[/blue] to "
            f"[green]{self.green_database}[/green]"
        )
        dict_cursor = self.con.cursor(DictCursor)
        dict_cursor.execute(f"show schemas in database {blue_database};")
        schemas = [
            schema
            for schema in dict_cursor.fetchall()
            if schema["name"] not in self._list_of_schemas_to_exclude
        ]

        # Read production's grants and check they can all be recreated before
        # cloning anything, so an unsupported grant fails fast. The database's own
        # grants are only checked here; they're re-read and copied after the build.
        _, _, unsupported = self._grant_sqls(
            self._show_database_grants(blue_database), green_database
        )
        post_build = PostBuildGrants()
        grants = []
        for schema in schemas:
            blue_schema = f"{blue_database}.{_quote_identifier(schema['name'])}"
            green_schema = f"{green_database}.{_quote_identifier(schema['name'])}"
            # A schema clone doesn't keep MANAGED ACCESS; losing it would let object
            # owners grant access on their own objects.
            if "MANAGED ACCESS" in (schema.get("options") or "").upper():
                post_build.managed_access.append(
                    f"alter schema {green_schema} enable managed access;"
                )
            dict_cursor.execute(f"show grants on schema {blue_schema}")
            schema_grants, ownership, schema_unsupported = self._grant_sqls(
                dict_cursor.fetchall(), green_schema
            )
            grants += schema_grants
            post_build.schema_ownership += ownership
            unsupported += schema_unsupported
            future, future_ownership = self._future_grant_sqls(
                "SCHEMA", blue_schema, green_schema
            )
            grants += future
            post_build.future_ownership += future_ownership
        self._raise_unsupported(unsupported)

        self._run_commands(
            [
                f"create schema {green_database}.{_quote_identifier(schema['name'])} "
                f"clone {blue_database}.{_quote_identifier(schema['name'])};"
                for schema in schemas
            ]
        )
        console.print(f"Cloned schemas in {time.time() - self.time_check} seconds.")
        self.time_check = time.time()

        console.print(
            f"Cloning [u]schema grants[/u] from [blue]{self.blue_database}[/blue] to "
            f"[green]{self.green_database}[/green]"
        )
        self._run_commands(grants)
        console.print(
            f"Cloned grants to schemas in {time.time() - self.time_check} seconds."
        )
        self.time_check = time.time()
        return post_build

    def clone_database_container_grants(
        self, blue_database: str, green_database: str
    ) -> PostBuildGrants:
        """
        Copies the database's future grants (except OWNERSHIP, applied after the
        build) and inherited grants. These belong to the database object itself, so
        without this they're lost when it's swapped out. Runs before the build so
        future grants apply to the objects dbt creates.

        Args:
            blue_database: The name of the blue database (prod)
            green_database: The name of the green database (staging).

        Returns:
            The future OWNERSHIP grants to apply after the build.
        """
        console.print(
            f"Cloning [u]future and inherited grants[/u] from "
            f"[blue]{blue_database}[/blue] to [green]{green_database}[/green]"
        )
        future, future_ownership = self._future_grant_sqls(
            "DATABASE", blue_database, green_database
        )
        self._run_commands(
            future + self._inherited_grant_sqls(blue_database, green_database)
        )
        return PostBuildGrants(future_ownership=future_ownership)

    def clone_database_grants(
        self, blue_database: str, green_database: str, post_build: PostBuildGrants
    ):
        """
        Runs after the build: copies the database's grants (re-read, as they may
        have changed during the build), then applies what the pre-build steps held
        back (future OWNERSHIP grants, MANAGED ACCESS) and finally transfers schema
        and database ownership.

        Args:
            blue_database: The name of the blue database (prod)
            green_database: The name of the green database (staging).
            post_build: What clone_database_schemas and
                clone_database_container_grants returned.

        Returns:
            None
        """
        console.print(
            f"Cloning grants from [blue]{self.blue_database}[/blue] to "
            f"green [green]{self.green_database}[/green]"
        )
        grants, database_ownership, unsupported = self._grant_sqls(
            self._show_database_grants(blue_database), green_database
        )
        # Checked before the build too; this catches one added during the build.
        self._raise_unsupported(unsupported)
        self._run_commands(grants + post_build.future_ownership)
        self._run_commands(post_build.managed_access)
        self._run_commands(post_build.schema_ownership)
        self._run_commands(database_ownership)


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
