CREATE FOREIGN TABLE kafka_test_prod (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int,
    some_text text,
    some_date date,
    some_time timestamp
)
SERVER kafka_server OPTIONS
    (format 'csv', topic 'contrib_regress_prod', batch_size '30', buffer_delay '100');

INSERT INTO kafka_test_prod(some_int, some_text, some_date)
    VALUES
    (1,'foo bar 1','2017-01-01'),
    (2,'foo text 2','2017-01-02'),
    (3,'foo text 3','2017-01-03'),
    (4,'foo text 4','2017-01-04'),
    (5,'foo text 5','2017-01-05'),
    (6,'foo text 6','2017-01-06'),
    (7,'foo bar 7','2017-01-07'),
    (8,'foo text 8','2017-01-08'),
    (9,'foo text 9','2017-01-09'),
    (10,'foo text 10','2017-01-10')

RETURNING *;


SELECT * FROM kafka_test_prod WHERE offs >= 0 and part=0;

INSERT INTO kafka_test_prod(some_int, some_text, some_date, some_time)
SELECT i,
    'It''s some text, that is for number '||i,
    ('2015-01-01'::date + (i || ' minutes')::interval)::date,
    ('2015-01-01'::date + (i || ' minutes')::interval)::timestamp
FROM generate_series(1,1e6::int, 10) i ORDER BY i;


SELECT * FROM kafka_test_prod WHERE offs >= 1000 and part=0 LIMIT 10;