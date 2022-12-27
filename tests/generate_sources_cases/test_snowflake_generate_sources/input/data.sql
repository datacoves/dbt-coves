INSERT INTO tests.test_model

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
