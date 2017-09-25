-- standard setup
CREATE SERVER kafka_server
FOREIGN DATA WRAPPER kafka_fdw
OPTIONS (brokers 'localhost:9092');

CREATE USER MAPPING FOR PUBLIC SERVER kafka_server;

CREATE FOREIGN TABLE kafka_test (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int,
    some_text text,
    some_date date,
    some_time timestamp
)

SERVER kafka_server OPTIONS 
    (format 'csv', topic 'contrib_regress', batch_size '30', buffer_delay '100');


-- check that we parse the queries right or error out if needed 
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test WHERE offs = 1 AND part = 0;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test WHERE part=1 AND some_int = 3 AND offs > 4 AND offs > 10 AND offs < 100;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test WHERE (part,offs)=(1,1) OR (part,offs)=(2,1);
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test WHERE (part,offs) IN((1,1),(2,2)) ;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test WHERE (part,offs) IN((1,1),(1,2)) ;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test WHERE some_int = 5 ;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test WHERE offs > '5' AND part = '1' ;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test WHERE  '5' < offs AND '1' = part ;

-- check that we really have messages
SELECT SUM(count) FROM (
SELECT COUNT(*) FROM kafka_test WHERE part = 0 AND offs >= 0
UNION ALL
SELECT COUNT(*) FROM kafka_test WHERE part = 1 AND offs >= 0
UNION ALL
SELECT COUNT(*) FROM kafka_test WHERE part = 2 AND offs >= 0
UNION ALL
SELECT COUNT(*) FROM kafka_test WHERE part = 3 AND offs >= 0
UNION ALL
SELECT COUNT(*) FROM kafka_test WHERE part = 4 AND offs >= 0
)t;

-- see that we can properly parse messages
SELECT * FROM kafka_test WHERE part = 0 AND offs > 10 LIMIT 60;
SELECT * FROM kafka_test WHERE part = 1 AND offs > 100 LIMIT 60;
SELECT * FROM kafka_test WHERE part = 2 AND offs > 1000 LIMIT 60;
SELECT * FROM kafka_test WHERE part = 3 AND offs > 1000 LIMIT 60;
SELECT * FROM kafka_test WHERE part = 4 AND offs > 0 LIMIT 60;

