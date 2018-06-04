\i test/sql/setup.inc
CREATE SERVER kafka_server2
FOREIGN DATA WRAPPER kafka_fdw
OPTIONS (brokers 'localhost:9092');

CREATE USER MAPPING FOR PUBLIC SERVER kafka_server2;

-- check that errornous setup is kept

-- unsupported format should error out
BEGIN;
CREATE FOREIGN TABLE kafka_err_test (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int,
    some_text text,
    some_date date,
    some_time timestamp
)
SERVER kafka_server2 OPTIONS
    (format 'foo', batch_size '30', buffer_delay '100', topic 'foo');
ROLLBACK;


-- redundant format should error out
BEGIN;
CREATE FOREIGN TABLE kafka_err_test (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int,
    some_text text,
    some_date date,
    some_time timestamp
)
SERVER kafka_server2 OPTIONS
    (format 'csv', topic 'foo', batch_size '30', buffer_delay '100', format 'json');
ROLLBACK;

-- no topic should error out
BEGIN;
CREATE FOREIGN TABLE kafka_err_test (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int,
    some_text text,
    some_date date,
    some_time timestamp
)
SERVER kafka_server2 OPTIONS
    (format 'csv', batch_size '30', buffer_delay '100');
ROLLBACK;
