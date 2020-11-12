\i test/sql/setup.inc

CREATE TYPE my_type AS (a int, b text);

CREATE FOREIGN TABLE kafka_test_prod_json (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int,
    some_text text,
    some_date date,
    some_time timestamp,
    some_timetz timestamptz,
    some_array text[],
    some_custom_type my_type,
    some_json jsonb
)
SERVER kafka_server OPTIONS
    (format 'json', topic 'contrib_regress_prod_json', batch_size '3000', buffer_delay '100');

INSERT INTO kafka_test_prod_json(part, some_int, some_text, some_date, some_time, some_timetz, some_json, some_array)
    VALUES
    (1, 1,'foo bar 1',   '2017-01-01', '2017-01-01 00:00:01', '2017-01-01 00:00:01', '{"a": 1, "b": 10}', array[1,2,3]),
    (1, 2,'foo text 2',  '2017-01-02', '2017-01-02 00:00:01', '2017-01-02 00:00:01', '{"a": 2, "b": 9}',  array[[1,2], [3,4]]),
    (1, 3,'foo text 3',  '2017-01-03', '2017-01-03 00:00:01', '2017-01-03 00:00:01', '{"a": 3, "b": 8}',  array['test [brackets]', 'test "[brackets]" in quotes']),
    (1, 4,'foo text 4',  '2017-01-04', '2017-01-04 00:00:01', '2017-01-04 00:00:01', '{"a": 4, "b": 7}',  NULL),
    (1, 5,'foo text 5',  '2017-01-05', '2017-01-05 00:00:01', '2017-01-05 00:00:01', '{"a": 5, "b": 6}',  NULL),
    (1, 6,'foo text 6',  '2017-01-06', '2017-01-06 00:00:01', '2017-01-06 00:00:01', '{"a": 6, "b": 5}',  NULL),
    (1, 7,'foo bar 7',   '2017-01-07', '2017-01-07 00:00:01', '2017-01-07 00:00:01', '{"a": 7, "b": 4}',  NULL),
    (1, 8,'foo text 8',  '2017-01-08', '2017-01-08 00:00:01', '2017-01-08 00:00:01', '{"a": 8, "b": 3}',  NULL),
    (1, 9,'foo text 9',  '2017-01-09', '2017-01-09 00:00:01', '2017-01-09 00:00:01', '{"a": 9, "b": 2}',  NULL),
    (1, 10,'foo text 10','2017-01-10', '2017-01-10 00:00:01', '2017-01-10 00:00:01', '{"a": 10, "b": 1}', NULL)

RETURNING *;

-- run some memload
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;


SELECT * FROM kafka_test_prod_json WHERE offs >= 0 and part=1;

INSERT INTO kafka_test_prod_json(some_int, some_text, some_date, some_time)
SELECT i,
    'It''s some text, that is for number '||i,
    ('2015-01-01'::date + (i || ' minutes')::interval)::date,
    ('2015-01-01'::date + (i || ' minutes')::interval)::timestamp
FROM generate_series(1,1e4::int, 10) i ORDER BY i;


--- check total was inserted
SELECT SUM(count) FROM(
SELECT COUNT(*) FROM kafka_test_prod_json WHERE offs >= 0 and part=0
UNION ALL
SELECT COUNT(*) FROM kafka_test_prod_json WHERE offs >= 0 and part=1
UNION ALL
SELECT COUNT(*) FROM kafka_test_prod_json WHERE offs >= 0 and part=2
UNION ALL
SELECT COUNT(*) FROM kafka_test_prod_json WHERE offs >= 0 and part=3
)t;

--- check auto distribution makes sense
SELECT COUNT(*) BETWEEN 100 AND 400 FROM kafka_test_prod_json WHERE offs >= 0 and part=0;
SELECT COUNT(*) BETWEEN 100 AND 400 FROM kafka_test_prod_json WHERE offs >= 0 and part=1;
SELECT COUNT(*) BETWEEN 100 AND 400 FROM kafka_test_prod_json WHERE offs >= 0 and part=2;
SELECT COUNT(*) BETWEEN 100 AND 400 FROM kafka_test_prod_json WHERE offs >= 0 and part=3;


--- check data is readable
SELECT some_int, some_text, some_date FROM(
(SELECT some_int, some_text, some_date FROM kafka_test_prod_json WHERE offs >= 0 and part=0 AND some_int = 231 LIMIT 1)
UNION ALL
(SELECT some_int, some_text, some_date FROM kafka_test_prod_json WHERE offs >= 0 and part=1 AND some_int = 231 LIMIT 1)
UNION ALL
(SELECT some_int, some_text, some_date FROM kafka_test_prod_json WHERE offs >= 0 and part=2 AND some_int = 231 LIMIT 1)
UNION ALL
(SELECT some_int, some_text, some_date FROM kafka_test_prod_json WHERE offs >= 0 and part=3 AND some_int = 231 LIMIT 1)
)t;


--- check exporting composite types to json format (importing is not yet supported)
INSERT INTO kafka_test_prod_json (some_custom_type) VALUES ((1, 'test'));
