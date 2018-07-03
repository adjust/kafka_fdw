\i test/sql/setup.inc
CREATE FOREIGN TABLE kafka_test_custom (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    len int
)
SERVER kafka_server OPTIONS
    (format 'custom', decode_lib '$libdir/kafka_fdw', decode_func 'kafka_custom_msglen' ,topic 'contrib_regress', batch_size '30', buffer_delay '500');

SELECT part, offs, len  FROM kafka_test_custom WHERE offs BETWEEN 1 AND 5 ORDER BY offs;