\i test/sql/setup.inc
show datestyle;

CREATE FOREIGN TABLE kafka_test_prod (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int,
    some_text text,
    some_date date,
    some_time timestamp
)
SERVER kafka_server OPTIONS
    (format 'csv', topic 'contrib_regress_prod', batch_size '3000', buffer_delay '100');

INSERT INTO kafka_test_prod(part, some_int, some_text, some_date)
    VALUES
    (1, 1,'foo bar 1','2017-01-01'),
    (1, 2,'foo text 2','2017-01-02'),
    (1, 3,'foo text 3','2017-01-03'),
    (1, 4,'foo text 4','2017-01-04'),
    (1, 5,'foo text 5','2017-01-05'),
    (1, 6,'foo text 6','2017-01-06'),
    (1, 7,'foo bar 7','2017-01-07'),
    (1, 8,'foo text 8','2017-01-08'),
    (1, 9,'foo text 9','2017-01-09'),
    (1, 10,'foo text 10','2017-01-10')

RETURNING *;

-- run some memload
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;


SELECT * FROM kafka_test_prod WHERE offs >= 0 and part=1;

INSERT INTO kafka_test_prod(some_int, some_text, some_date, some_time)
SELECT i,
    'It''s some text, that is for number '||i,
    ('2015-01-01'::date + (i || ' minutes')::interval)::date,
    ('2015-01-01'::date + (i || ' minutes')::interval)::timestamp
FROM generate_series(1,1e4::int, 10) i ORDER BY i;

EXPLAIN (COSTS OFF) INSERT INTO kafka_test_prod(some_int, some_text, some_date, some_time)
VALUES (1, 'test', NULL, NULL);


--- check total was inserted
SELECT SUM(count) FROM(
SELECT COUNT(*) FROM kafka_test_prod WHERE offs >= 0 and part=0
UNION ALL
SELECT COUNT(*) FROM kafka_test_prod WHERE offs >= 0 and part=1
UNION ALL
SELECT COUNT(*) FROM kafka_test_prod WHERE offs >= 0 and part=2
UNION ALL
SELECT COUNT(*) FROM kafka_test_prod WHERE offs >= 0 and part=3
)t;

--- check auto distribution makes sense
SELECT COUNT(*) BETWEEN 120 AND 350 FROM kafka_test_prod WHERE offs >= 0 and part=0;
SELECT COUNT(*) BETWEEN 120 AND 350 FROM kafka_test_prod WHERE offs >= 0 and part=1;
SELECT COUNT(*) BETWEEN 120 AND 350 FROM kafka_test_prod WHERE offs >= 0 and part=2;
SELECT COUNT(*) BETWEEN 120 AND 350 FROM kafka_test_prod WHERE offs >= 0 and part=3;


--- check data is readable
SELECT some_int, some_text, some_date FROM(
(SELECT some_int, some_text, some_date FROM kafka_test_prod WHERE offs >= 0 and part=0 AND some_int = 231 LIMIT 1)
UNION ALL
(SELECT some_int, some_text, some_date FROM kafka_test_prod WHERE offs >= 0 and part=1 AND some_int = 231 LIMIT 1)
UNION ALL
(SELECT some_int, some_text, some_date FROM kafka_test_prod WHERE offs >= 0 and part=2 AND some_int = 231 LIMIT 1)
UNION ALL
(SELECT some_int, some_text, some_date FROM kafka_test_prod WHERE offs >= 0 and part=3 AND some_int = 231 LIMIT 1)
)t;
