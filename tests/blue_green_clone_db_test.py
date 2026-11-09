"""
Unit tests for the SQL CloneDB issues during a blue-green deploy. A fake Snowflake
connection returns canned SHOW results and records every statement executed.
"""

import itertools

import pytest

from dbt_coves.tasks.blue_green.clone_db import (
    CloneDB,
    _object_type_plural,
    _sql_string,
)

BLUE = "PROD"
GREEN = "PROD_STAGING"


class FakeCursor:
    def __init__(self, con):
        self.con = con
        self._rows = []
        self.sfqid = None

    def execute(self, sql):
        self.con.executed.append(sql)
        self._rows = self.con.show_results.get(sql.rstrip(";").strip().lower(), [])
        return self

    def execute_async(self, sql):
        self.con.executed.append(sql)
        self.sfqid = next(self.con._ids)

    def fetchall(self):
        return self._rows


class FakeConnection:
    def __init__(self, show_results=None):
        self.show_results = {k.lower(): v for k, v in (show_results or {}).items()}
        self.executed = []
        self._ids = itertools.count()

    def cursor(self, cursor_class=None):
        return FakeCursor(self)

    def get_query_status_throw_if_error(self, qid):
        return "SUCCESS"

    def is_still_running(self, status):
        return False


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    monkeypatch.setattr(
        "dbt_coves.tasks.blue_green.clone_db.time.sleep", lambda s: None
    )


def grants_issued(con):
    return [sql for sql in con.executed if sql.upper().startswith("GRANT")]


def test_helpers():
    assert _sql_string("it's a \\ test") == "'it''s a \\\\ test'"
    assert _object_type_plural("TABLE") == "TABLES"
    assert _object_type_plural("MATERIALIZED_VIEW") == "MATERIALIZED VIEWS"
    assert _object_type_plural("PASSWORD_POLICY") == "PASSWORD POLICIES"


def test_drop_database_if_exists_flag():
    con = FakeConnection()
    cdb = CloneDB(BLUE, GREEN, con)
    cdb.drop_database()
    cdb.drop_database(if_exists=False)
    assert con.executed == [
        f"drop database if exists {GREEN};",
        f"drop database {GREEN};",
    ]


def test_clone_database_properties_copies_database_level_parameters_and_comment():
    con = FakeConnection(
        {
            f"show parameters in database {BLUE}": [
                {
                    "key": "MAX_DATA_EXTENSION_TIME_IN_DAYS",
                    "value": "30",
                    "level": "DATABASE",
                    "type": "NUMBER",
                },
                {
                    "key": "DEFAULT_DDL_COLLATION",
                    "value": "en-ci",
                    "level": "DATABASE",
                    "type": "STRING",
                },
                # Inherited from the account or defaulted: not copied.
                {
                    "key": "DATA_RETENTION_TIME_IN_DAYS",
                    "value": "1",
                    "level": "ACCOUNT",
                    "type": "NUMBER",
                },
                {"key": "LOG_LEVEL", "value": "OFF", "level": "", "type": "STRING"},
            ],
            f"show databases like '{BLUE}'": [
                # LIKE treats _ as a wildcard, so near-matches must be ignored.
                {"name": "PRODX", "comment": "wrong one"},
                {"name": BLUE, "comment": "Production's database"},
            ],
        }
    )
    CloneDB(BLUE, GREEN, con).clone_database_properties(BLUE, GREEN)
    assert con.executed[1:3] == [
        f"alter database {GREEN} set MAX_DATA_EXTENSION_TIME_IN_DAYS = 30;",
        f"alter database {GREEN} set DEFAULT_DDL_COLLATION = 'en-ci';",
    ]
    assert con.executed[-1] == (
        f"comment on database {GREEN} is 'Production''s database';"
    )
    assert not any(
        "DATA_RETENTION" in sql or "LOG_LEVEL" in sql for sql in con.executed
    )


def test_clone_database_grants_copies_future_and_inherited_grants_ownership_last():
    con = FakeConnection(
        {
            f"show grants on database {BLUE}": [
                {
                    "privilege": "OWNERSHIP",
                    "granted_on": "DATABASE",
                    "grantee_name": "DEPLOYER",
                },
                {
                    "privilege": "USAGE",
                    "granted_on": "DATABASE",
                    "grantee_name": "READER",
                },
            ],
            f"show future grants in database {BLUE}": [
                {
                    "privilege": "SELECT",
                    "grant_on": "TABLE",
                    "grant_to": "ROLE",
                    "grantee_name": "READER",
                    "grant_option": "false",
                },
                {
                    "privilege": "USAGE",
                    "grant_on": "SCHEMA",
                    "grant_to": "ROLE",
                    "grantee_name": "ADMIN",
                    "grant_option": "true",
                },
                {
                    "privilege": "SELECT",
                    "grant_on": "VIEW",
                    "grant_to": "DATABASE_ROLE",
                    "grantee_name": "PROD.DB_READER",
                    "grant_option": "false",
                },
            ],
            # Columns as returned by Snowflake (verified against a live account).
            f"show inherited grants in database {BLUE}": [
                {
                    "privilege": "SELECT",
                    "granted_on": "DYNAMIC_TABLE",
                    "inherited_from": "DATABASE",
                    "inherited_from_database": BLUE,
                    "inherited_from_schema": "",
                    "granted_to": "ROLE",
                    "grantee_name": "READER",
                },
                {
                    "privilege": "SELECT",
                    "granted_on": "TABLE",
                    "inherited_from": "SCHEMA",
                    "inherited_from_database": BLUE,
                    "inherited_from_schema": "RAW",
                    "granted_to": "ROLE",
                    "grantee_name": "READER",
                },
                {
                    "privilege": "SELECT",
                    "granted_on": "VIEW",
                    "inherited_from": "DATABASE",
                    "inherited_from_database": BLUE,
                    "inherited_from_schema": "",
                    "granted_to": "DATABASE_ROLE",
                    "grantee_name": "PROD.DB_READER",
                },
            ],
        }
    )
    CloneDB(BLUE, GREEN, con).clone_database_grants(BLUE, GREEN)
    grants = grants_issued(con)
    assert sorted(grants[:-1]) == sorted(
        [
            f"GRANT USAGE ON DATABASE {GREEN} TO ROLE READER;",
            f"GRANT SELECT ON FUTURE TABLES IN DATABASE {GREEN} TO ROLE READER;",
            f"GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {GREEN} TO ROLE ADMIN "
            "WITH GRANT OPTION;",
            f"GRANT INHERITED SELECT ON ALL DYNAMIC TABLES IN DATABASE {GREEN} "
            "TO ROLE READER;",
        ]
    )
    assert grants[-1] == (
        f"GRANT OWNERSHIP ON DATABASE {GREEN} TO ROLE DEPLOYER COPY CURRENT GRANTS;"
    )


def test_clone_database_schemas_restores_managed_access_and_container_grants():
    con = FakeConnection(
        {
            f"show schemas in database {BLUE}": [
                {"name": "MANAGED", "options": "MANAGED ACCESS"},
                {"name": "PLAIN", "options": ""},
                {"name": "PUBLIC", "options": "MANAGED ACCESS"},  # excluded
            ],
            f"show grants on schema {BLUE}.MANAGED": [
                {
                    "privilege": "OWNERSHIP",
                    "granted_on": "SCHEMA",
                    "grantee_name": "OWNER",
                },
                {
                    "privilege": "USAGE",
                    "granted_on": "SCHEMA",
                    "grantee_name": "READER",
                },
            ],
            f"show future grants in schema {BLUE}.MANAGED": [
                {
                    "privilege": "SELECT",
                    "grant_on": "DYNAMIC_TABLE",
                    "grant_to": "ROLE",
                    "grantee_name": "READER",
                    "grant_option": "false",
                }
            ],
            f"show inherited grants in schema {BLUE}.MANAGED": [
                {
                    "privilege": "SELECT",
                    "granted_on": "VIEW",
                    "inherited_from": "SCHEMA",
                    "inherited_from_database": BLUE,
                    "inherited_from_schema": "MANAGED",
                    "granted_to": "ROLE",
                    "grantee_name": "READER",
                }
            ],
        }
    )
    CloneDB(BLUE, GREEN, con).clone_database_schemas(BLUE, GREEN)
    executed = con.executed

    assert f"create schema {GREEN}.MANAGED clone {BLUE}.MANAGED;" in executed
    assert f"create schema {GREEN}.PLAIN clone {BLUE}.PLAIN;" in executed
    assert not any(f"{GREEN}.PUBLIC" in sql for sql in executed)

    managed = f"alter schema {GREEN}.MANAGED enable managed access;"
    assert managed in executed
    assert not any("PLAIN enable managed access" in sql for sql in executed)

    grants = grants_issued(con)
    assert sorted(grants[:-1]) == sorted(
        [
            f"GRANT USAGE ON SCHEMA {GREEN}.MANAGED TO ROLE READER;",
            f"GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA {GREEN}.MANAGED "
            "TO ROLE READER;",
            f"GRANT INHERITED SELECT ON ALL VIEWS IN SCHEMA {GREEN}.MANAGED "
            "TO ROLE READER;",
        ]
    )
    assert grants[-1] == (
        f"GRANT OWNERSHIP ON SCHEMA {GREEN}.MANAGED TO ROLE OWNER COPY CURRENT GRANTS;"
    )
    # Managed access is enabled before any grant (and before ownership moves away).
    assert executed.index(managed) < executed.index(grants[0])


def test_inherited_grants_skipped_when_show_fails():
    from snowflake.connector.errors import ProgrammingError

    con = FakeConnection()
    original_cursor = con.cursor

    def cursor(cursor_class=None):
        cur = original_cursor(cursor_class)
        execute = cur.execute

        def failing_execute(sql):
            if sql.startswith("show inherited grants"):
                raise ProgrammingError("Unsupported feature 'INHERITED GRANTS'")
            return execute(sql)

        cur.execute = failing_execute
        return cur

    con.cursor = cursor
    CloneDB(BLUE, GREEN, con).clone_database_grants(BLUE, GREEN)
    assert grants_issued(con) == []
