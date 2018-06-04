\i test/sql/setup.inc
CREATE SERVER kafka_server_multi
FOREIGN DATA WRAPPER kafka_fdw
OPTIONS (brokers 'foo:1234,localhost:9092,bar:9784');
CREATE USER MAPPING FOR PUBLIC SERVER kafka_server_multi;

CREATE FOREIGN TABLE kafka_test_part_multi (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int,
    some_text text,
    some_date date,
    some_time timestamp
)
SERVER kafka_server_multi OPTIONS
    (format 'csv', topic 'contrib_regress', batch_size '30', buffer_delay '100');

SELECT * FROM kafka_test_part_multi WHERE part = 0 AND offs > 1000 LIMIT 30;
