CREATE DATABASE IF NOT EXISTS DBT_COVES_TEST;
USE DATABASE DBT_COVES_TEST;

GRANT USAGE ON DATABASE DBT_COVES_TEST TO ROLE TRANSFORMER_DBT;

CREATE SCHEMA IF NOT EXISTS DBT_COVES_TEST.TESTS_BLUE_GREEN;

CREATE TABLE IF NOT EXISTS DBT_COVES_TEST.TESTS_BLUE_GREEN.TEST_MODEL (
    test_smallint SMALLINT,
    test_integer INTEGER,
    test_bigint BIGINT,
    test_decimal DECIMAL,
    test_numeric NUMERIC,
    test_char CHAR,
    test_varchar VARCHAR,
    test_date DATE,
    test_timestamp TIMESTAMP,
    test_boolean BOOLEAN,
    test_json VARIANT
);

INSERT INTO DBT_COVES_TEST.TESTS_BLUE_GREEN.TEST_MODEL

SELECT
    1,
    1,
    1,
    1.1,
    1.1,
    'a',
    'a',
    '2020-01-01',
    '2020-01-01 00:00:00',
    true,
    parse_json($${"json_value_1":"abc","json_value_2": 2}$$);
