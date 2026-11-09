"""
Unit tests for the SQL CloneDB issues during a blue-green deploy. A fake Snowflake
connection returns canned SHOW results and records every statement executed.
"""

import copy
import itertools

import pytest
from snowflake.connector.errors import ProgrammingError

from dbt_coves.tasks.blue_green.clone_db import (
    CloneDB,
    UnsupportedGrantError,
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
        key = sql.rstrip(";").strip().lower()
        if any(key.startswith(prefix) for prefix in self.con.fail_on):
            raise ProgrammingError(f"failed: {sql}")
        self._rows = self.con.show_results.get(key, [])
        return self

    def execute_async(self, sql):
        self.con.executed.append(sql)
        self.sfqid = next(self.con._ids)

    def fetchall(self):
        return self._rows


class FakeConnection:
    def __init__(self, show_results=None, fail_on=()):
        self.show_results = {k.lower(): v for k, v in (show_results or {}).items()}
        self.fail_on = [prefix.lower() for prefix in fail_on]
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
    assert _object_type_plural("GATEWAY") == "GATEWAYS"


def test_drop_database_if_exists_flag():
    con = FakeConnection()
    cdb = CloneDB(BLUE, GREEN, con)
    cdb.drop_database()
    cdb.drop_database(if_exists=False)
    assert con.executed == [
        f"drop database if exists {GREEN};",
        f"drop database {GREEN};",
    ]


def parameter(key, value, level="DATABASE", type_="STRING"):
    return {"key": key, "value": value, "level": level, "type": type_}


def test_clone_database_properties_copies_database_level_parameters_and_comment():
    con = FakeConnection(
        {
            f"show parameters in database {BLUE}": [
                parameter("MAX_DATA_EXTENSION_TIME_IN_DAYS", "30", type_="NUMBER"),
                parameter("DEFAULT_DDL_COLLATION", "en-ci"),
                # The deploy role can't set this one (needs MODIFY LOG LEVEL).
                parameter("LOG_LEVEL", "WARN"),
                # Inherited from the account or defaulted: not copied.
                parameter("DATA_RETENTION_TIME_IN_DAYS", "1", "ACCOUNT", "NUMBER"),
                parameter("TRACE_LEVEL", "OFF", level=""),
            ],
            f"show databases like '{BLUE}'": [
                # LIKE treats _ as a wildcard, so near-matches must be ignored.
                {"name": "PRODX", "comment": "wrong one"},
                {"name": BLUE, "comment": "Production's database"},
            ],
        },
        fail_on=[f"alter database {GREEN} set LOG_LEVEL"],
    )
    CloneDB(BLUE, GREEN, con).clone_database_properties(BLUE, GREEN)

    alters = [sql for sql in con.executed if sql.startswith("alter database")]
    # The failing parameter is skipped (with a warning) and the rest still applied.
    assert alters == [
        f"alter database {GREEN} set MAX_DATA_EXTENSION_TIME_IN_DAYS = 30;",
        f"alter database {GREEN} set DEFAULT_DDL_COLLATION = 'en-ci';",
        f"alter database {GREEN} set LOG_LEVEL = 'WARN';",
    ]
    assert con.executed[-1] == (
        f"comment on database {GREEN} is 'Production''s database';"
    )


def role_grant(privilege, granted_on, name, grantee, granted_to="ROLE", option=None):
    if option is None:
        option = "true" if privilege == "OWNERSHIP" else "false"
    return {
        "privilege": privilege,
        "granted_on": granted_on,
        "name": name,
        "granted_to": granted_to,
        "grantee_name": grantee,
        "grant_option": option,
    }


def future_grant(privilege, grant_on, grantee, grant_to="ROLE", grant_option="false"):
    return {
        "privilege": privilege,
        "grant_on": grant_on,
        "grant_to": grant_to,
        "grantee_name": grantee,
        "grant_option": grant_option,
    }


def inherited_grant(privilege, granted_on, grantee, inherited_from="DATABASE", **kw):
    # Columns as returned by Snowflake (verified against a live account).
    return {
        "privilege": privilege,
        "granted_on": granted_on,
        "inherited_from": inherited_from,
        "inherited_from_database": BLUE,
        "inherited_from_schema": kw.get("schema", ""),
        "granted_to": kw.get("granted_to", "ROLE"),
        "grantee_name": grantee,
    }


# A production database with a managed and a plain schema, and grants of every kind
# blue-green can recreate.
PROD_SHOW_RESULTS = {
    f"show schemas in database {BLUE}": [
        {"name": "MANAGED", "options": "MANAGED ACCESS"},
        {"name": "PLAIN", "options": ""},
        {"name": "PUBLIC", "options": "MANAGED ACCESS"},  # excluded from cloning
    ],
    f'show grants on schema {BLUE}."MANAGED"': [
        role_grant("OWNERSHIP", "SCHEMA", f"{BLUE}.MANAGED", "OWNER"),
        role_grant("USAGE", "SCHEMA", f"{BLUE}.MANAGED", "READER"),
        role_grant("USAGE", "SCHEMA", f"{BLUE}.MANAGED", "ADMIN", option="true"),
    ],
    f'show future grants in schema {BLUE}."MANAGED"': [
        future_grant("SELECT", "DYNAMIC_TABLE", "READER"),
        future_grant("OWNERSHIP", "TABLE", "OWNER", grant_option="true"),
    ],
    f"show grants on database {BLUE}": [
        role_grant("OWNERSHIP", "DATABASE", BLUE, "DEPLOYER"),
        role_grant("USAGE", "DATABASE", BLUE, "READER"),
    ],
    f"show future grants in database {BLUE}": [
        future_grant("SELECT", "TABLE", "READER"),
        future_grant("USAGE", "SCHEMA", "ADMIN", grant_option="true"),
        # Future/inherited grants to database roles were never copied: skipped
        # with a warning.
        future_grant("SELECT", "VIEW", "PROD.DB_READER", grant_to="DATABASE_ROLE"),
        future_grant("OWNERSHIP", "VIEW", "OWNER", grant_option="true"),
    ],
    f"show inherited grants in database {BLUE}": [
        inherited_grant("SELECT", "DYNAMIC_TABLE", "READER"),
        # Schema-level rows aren't copied at database level (a schema clone keeps them)
        inherited_grant("SELECT", "TABLE", "READER", "SCHEMA", schema="MANAGED"),
        inherited_grant("SELECT", "VIEW", "PROD.DB_READER", granted_to="DATABASE_ROLE"),
    ],
}

PRE_BUILD_GRANTS = [
    f'GRANT USAGE ON SCHEMA {GREEN}."MANAGED" TO ROLE "READER";',
    f'GRANT USAGE ON SCHEMA {GREEN}."MANAGED" TO ROLE "ADMIN" WITH GRANT OPTION;',
    f'GRANT SELECT ON FUTURE DYNAMIC TABLES IN SCHEMA {GREEN}."MANAGED" TO ROLE "READER";',
    f'GRANT SELECT ON FUTURE TABLES IN DATABASE {GREEN} TO ROLE "READER";',
    f'GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {GREEN} TO ROLE "ADMIN" '
    "WITH GRANT OPTION;",
    f'GRANT INHERITED SELECT ON ALL DYNAMIC TABLES IN DATABASE {GREEN} TO ROLE "READER";',
]
MANAGED_ACCESS = f'alter schema {GREEN}."MANAGED" enable managed access;'


def run_pre_build(con):
    cdb = CloneDB(BLUE, GREEN, con)
    post_build = cdb.clone_database_schemas(BLUE, GREEN)
    post_build += cdb.clone_database_container_grants(BLUE, GREEN)
    return cdb, post_build


def test_pre_build_clones_schemas_and_copies_non_ownership_grants():
    con = FakeConnection(PROD_SHOW_RESULTS)
    _, post_build = run_pre_build(con)

    assert f'create schema {GREEN}."MANAGED" clone {BLUE}."MANAGED";' in con.executed
    assert f'create schema {GREEN}."PLAIN" clone {BLUE}."PLAIN";' in con.executed
    assert not any(f'{GREEN}."PUBLIC"' in sql for sql in con.executed)

    # Everything except OWNERSHIP is in place before dbt creates objects.
    assert sorted(grants_issued(con)) == sorted(PRE_BUILD_GRANTS)
    # MANAGED ACCESS and ownership are held back and returned for after the build.
    assert MANAGED_ACCESS not in con.executed
    assert post_build.managed_access == [MANAGED_ACCESS]
    assert post_build.schema_ownership == [
        f'GRANT OWNERSHIP ON SCHEMA {GREEN}."MANAGED" TO ROLE "OWNER" COPY CURRENT GRANTS;'
    ]
    assert sorted(post_build.future_ownership) == sorted(
        [
            f'GRANT OWNERSHIP ON FUTURE TABLES IN SCHEMA {GREEN}."MANAGED" '
            'TO ROLE "OWNER";',
            f'GRANT OWNERSHIP ON FUTURE VIEWS IN DATABASE {GREEN} TO ROLE "OWNER";',
        ]
    )
    # Schema-level inherited grants survive the schema clone, so they aren't listed.
    assert not any("show inherited grants in schema" in sql for sql in con.executed)


def test_post_build_order_grants_then_managed_access_then_ownership():
    con = FakeConnection(PROD_SHOW_RESULTS)
    cdb, post_build = run_pre_build(con)
    con.executed.clear()

    cdb.clone_database_grants(BLUE, GREEN, post_build)
    issued = [sql for sql in con.executed if not sql.startswith("show ")]

    assert sorted(issued[:3]) == sorted(
        [f'GRANT USAGE ON DATABASE {GREEN} TO ROLE "READER";']
        + post_build.future_ownership
    )
    assert issued[3:] == [
        MANAGED_ACCESS,
        f'GRANT OWNERSHIP ON SCHEMA {GREEN}."MANAGED" TO ROLE "OWNER" COPY CURRENT GRANTS;',
        f'GRANT OWNERSHIP ON DATABASE {GREEN} TO ROLE "DEPLOYER" COPY CURRENT GRANTS;',
    ]


def test_names_from_show_output_are_quoted():
    con = FakeConnection(
        {
            f"show schemas in database {BLUE}": [{"name": "raw", "options": ""}],
            f'show grants on schema {BLUE}."raw"': [
                role_grant("USAGE", "SCHEMA", f'{BLUE}."raw"', "Data-Eng"),
                role_grant("USAGE", "SCHEMA", f'{BLUE}."raw"', 'odd"name'),
            ],
        }
    )
    CloneDB(BLUE, GREEN, con).clone_database_schemas(BLUE, GREEN)
    assert f'create schema {GREEN}."raw" clone {BLUE}."raw";' in con.executed
    assert sorted(grants_issued(con)) == sorted(
        [
            f'GRANT USAGE ON SCHEMA {GREEN}."raw" TO ROLE "Data-Eng";',
            f'GRANT USAGE ON SCHEMA {GREEN}."raw" TO ROLE "odd""name";',
        ]
    )


@pytest.mark.parametrize(
    "show, grant",
    [
        (
            f'show grants on schema {BLUE}."MANAGED"',
            role_grant(
                "USAGE", "SCHEMA", f"{BLUE}.MANAGED", "PROD.DB_READER", "DATABASE_ROLE"
            ),
        ),
        (
            f"show grants on database {BLUE}",
            role_grant("USAGE", "DATABASE", BLUE, "MY_SHARE", "SHARE"),
        ),
    ],
)
def test_unsupported_grantee_fails_before_the_build_without_granting(show, grant):
    results = copy.deepcopy(PROD_SHOW_RESULTS)
    results[show].append(grant)
    con = FakeConnection(results)

    with pytest.raises(UnsupportedGrantError) as error:
        run_pre_build(con)
    assert grant["grantee_name"] in str(error.value)
    # Raised before anything is cloned or granted.
    assert not any(sql.startswith("create schema") for sql in con.executed)
    assert grants_issued(con) == []


def test_inherited_grants_skipped_when_feature_disabled():
    con = FakeConnection(
        {
            "show parameters like 'FEATURE_RBAC_INHERITED_GRANTS' in account": [
                {"key": "FEATURE_RBAC_INHERITED_GRANTS", "value": "DISABLED"}
            ]
        },
        fail_on=["show inherited grants"],
    )
    CloneDB(BLUE, GREEN, con).clone_database_container_grants(BLUE, GREEN)
    assert grants_issued(con) == []


def test_inherited_grants_error_raised_when_feature_enabled():
    # e.g. the deploy role lacks the privilege to list them: skipping would
    # silently drop the grants, so the deploy fails instead.
    con = FakeConnection(
        {
            "show parameters like 'FEATURE_RBAC_INHERITED_GRANTS' in account": [
                {"key": "FEATURE_RBAC_INHERITED_GRANTS", "value": "ENABLED"}
            ]
        },
        fail_on=["show inherited grants"],
    )
    with pytest.raises(ProgrammingError):
        CloneDB(BLUE, GREEN, con).clone_database_container_grants(BLUE, GREEN)


@pytest.mark.parametrize(
    "names, exists",
    [
        ([GREEN], True),
        # A quoted lowercase "prod_staging" is a different database.
        ([GREEN.lower()], False),
        # SHOW DATABASES LIKE treats _ as a wildcard: a near-match isn't staging.
        (["PRODXSTAGING"], False),
        ([], False),
    ],
)
def test_check_if_staging_database_exists_matches_exact_name(names, exists):
    from dbt_coves.tasks.blue_green.main import BlueGreenTask

    task = BlueGreenTask.__new__(BlueGreenTask)
    task.staging_database = GREEN
    task.con = FakeConnection(
        {f"show databases like '{GREEN}'": [{"name": name} for name in names]}
    )
    assert task._check_if_database_exists() is exists


def test_inherited_grants_original_error_kept_when_feature_check_fails():
    con = FakeConnection(
        fail_on=["show inherited grants", "show parameters like"],
    )
    with pytest.raises(ProgrammingError, match="show inherited grants"):
        CloneDB(BLUE, GREEN, con).clone_database_container_grants(BLUE, GREEN)
