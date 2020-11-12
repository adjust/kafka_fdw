\i test/sql/setup.inc

-- standard setup
CREATE FOREIGN TABLE IF NOT EXISTS kafka_test_part (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int,
    some_text text,
    some_date date,
    some_time timestamp
)
SERVER kafka_server OPTIONS
    (format 'csv', topic 'contrib_regress4', batch_size '30', buffer_delay '100');

ALTER FOREIGN TABLE kafka_test_part OPTIONS(ADD num_partitions '4');

set max_parallel_workers_per_gather=2;
set max_parallel_workers=8;
set parallel_setup_cost=0;

ANALYZE kafka_test_part;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part ;
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE ((part = 1 or part = 2) and offs = 3) OR (part = 4 and offs=7);
EXPLAIN (COSTS OFF) SELECT * FROM kafka_test_part WHERE ((part = 1 or part = 2) and offs = 3) OR ((part = 4 and offs=7 ) or ( part = 5 and (offs = 10 or offs=12)) );

