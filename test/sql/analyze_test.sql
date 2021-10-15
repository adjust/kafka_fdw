\i test/sql/setup.inc

-- test ANALYZE
CREATE FOREIGN TABLE kafka_analyze_json (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int OPTIONS (json 'int_val'),
    some_text text OPTIONS (json 'text_val'),
    some_date date OPTIONS (json 'date_val'),
    some_time timestamp OPTIONS (json 'time_val')
)
SERVER kafka_server OPTIONS
    (format 'json', topic 'contrib_regress_json', batch_size '30', buffer_delay '500');
-- set default costs so that local settings won't affect the test
SET seq_page_cost     = 1.0;
SET cpu_tuple_cost    = 0.01;
SET cpu_operator_cost = 0.0025;
SET max_parallel_workers_per_gather = 0;
-- without statistics (in pg14 reltuples=-1 when uninitialized, and =0 in earlier versions)
SELECT reltuples > 0 FROM pg_class WHERE oid = 'kafka_analyze_json'::regclass;
-- with statistics
ANALYZE kafka_analyze_json;
SELECT reltuples FROM pg_class WHERE oid = 'kafka_analyze_json'::regclass;
