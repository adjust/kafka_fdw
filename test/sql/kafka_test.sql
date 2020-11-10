\i test/sql/setup.inc

/*
 * Returns EXPLAIN ANALYZE result without any arbitrary numbers like costs
 * or execution time.
 *
 * In Postgres 10 there is a very convinient feature EXPLAIN (SUMMARY OFF),
 * which removes 'Planning time' and 'Execution time' information lines from
 * the result. Unfortunately we cannot use it on older PostgreSQL versions.
 */
CREATE OR REPLACE FUNCTION explain(query TEXT) RETURNS SETOF RECORD AS $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN EXECUTE 'EXPLAIN (COSTS OFF, TIMING OFF, ANALYZE) ' || query
    LOOP
        RETURN NEXT rec;
    END LOOP;
END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION explain_invariant(query TEXT)
RETURNS SETOF text AS $$
DECLARE
    rec RECORD;
BEGIN
    FOR rec IN SELECT * FROM explain(query) as e(t text)
    LOOP
        IF position('planning time' in lower(rec.t)) > 0 OR
           position('execution time' in lower(rec.t)) > 0
        THEN
            CONTINUE;
        END IF;
        RETURN NEXT rec.t;
    END LOOP;
END
$$ LANGUAGE plpgsql;

-- standard setup
CREATE FOREIGN TABLE kafka_test_part (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int,
    some_text text,
    some_date date,
    some_time timestamp
)
SERVER kafka_server OPTIONS
    (format 'csv', topic 'contrib_regress4', batch_size '30', buffer_delay '100');

CREATE FOREIGN TABLE kafka_test_single_part (
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
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE offs = 1 AND part = 0;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE part=1 AND some_int = 3 AND offs > 4 AND offs > 10 AND offs < 100;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE part=1 AND some_int = 3 AND offs > 4 AND offs > 10 AND offs <= 100;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE (part,offs)=(1,1) OR (part,offs)=(2,1);
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE (part,offs)=(1,1) OR (part,offs)=(1,2);
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE (part,offs)=(1,1) OR (part,offs)=(2,2);
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE (part=1 or part=2) AND (offs=3 or offs=4);
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE (part,offs) IN((1,1),(2,2)) ;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE (part,offs) IN((1,1),(1,2)) ;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE some_int = 5 ;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE offs > 5 AND part = 1 ;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE  5 < offs AND 1 = part ;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE ((part = 1 or part = 2) and offs = 3) OR (part = 4 and offs=7);
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE ((part = 1 or part = 2) and offs = 3) OR ((part = 4 and offs=7 ) or ( part = 5 and (offs = 10 or offs=12)) );

-- check parameterized queries
SELECT explain_invariant('SELECT * FROM kafka_test_part WHERE offs = (SELECT 1) AND part = 0');
SELECT explain_invariant('SELECT * FROM kafka_test_part WHERE offs = 0 AND part = (SELECT 1)');
SELECT explain_invariant('SELECT * FROM kafka_test_part WHERE offs < (SELECT 1) AND part = 0');
SELECT explain_invariant('SELECT * FROM kafka_test_part WHERE offs <= (SELECT 1) AND part = 0');
SELECT explain_invariant('SELECT * FROM kafka_test_part WHERE offs = 0 AND part < (SELECT 1)');
SELECT explain_invariant('SELECT * FROM kafka_test_part WHERE offs = 0 AND part <= (SELECT 1)');

-- run some memload
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;
select count(*) from (select json_agg(s) from generate_series(1, 1000000) s) a;

-- check that we really have messages
SELECT SUM(count) FROM (
SELECT COUNT(*) FROM kafka_test_part WHERE part = 0 AND offs >= 0
UNION ALL
SELECT COUNT(*) FROM kafka_test_part WHERE part = 1 AND offs >= 0
UNION ALL
SELECT COUNT(*) FROM kafka_test_part WHERE part = 2 AND offs >= 0
UNION ALL
SELECT COUNT(*) FROM kafka_test_part WHERE part = 3 AND offs >= 0
UNION ALL
SELECT COUNT(*) FROM kafka_test_part WHERE part = 4 AND offs >= 0
)t;

SELECT COUNT(*) FROM kafka_test_part;

-- see that we can properly parse messages
SELECT * FROM kafka_test_single_part WHERE part = 0 AND offs > 0 LIMIT 30;
SELECT * FROM kafka_test_single_part WHERE part = 0 AND offs > 10 LIMIT 30;
SELECT * FROM kafka_test_single_part WHERE part = 0 AND offs > 100 LIMIT 30;
SELECT * FROM kafka_test_single_part WHERE part = 0 AND offs > 1000 LIMIT 30;
SELECT * FROM kafka_test_single_part WHERE part = 0 AND offs > 10000 LIMIT 30;
SELECT * FROM kafka_test_single_part WHERE part = 0 AND offs > 90000 LIMIT 30;
SELECT * FROM kafka_test_single_part WHERE part = 0 AND offs > 100000 LIMIT 30;



SELECT COUNT(*) FROM kafka_test_part WHERE (part = 1 AND offs BETWEEN 100 AND 200) OR ((part = 1 AND offs BETWEEN 500 AND 600) );
SELECT COUNT(*) FROM kafka_test_part WHERE (part = 1 AND offs BETWEEN 1 AND 2) OR ((part = 1 AND offs BETWEEN 5 AND 6) );

PREPARE kafka_test(int,bigint,bigint, int,bigint,bigint) AS SELECT COUNT(*) FROM kafka_test_part WHERE (part = $1 AND offs BETWEEN $2 AND $3) OR ((part = $4 AND offs BETWEEN $5 AND $6) );
EXPLAIN (COSTS OFF) EXECUTE kafka_test(1,100,200,1,500,600);
EXPLAIN (COSTS OFF) EXECUTE kafka_test(1,1,2,1,5,6);

-- test utils
SELECT SUM(offset_low), SUM(offset_high) FROM kafka_get_watermarks('kafka_test_part');
