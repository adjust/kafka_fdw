CREATE FOREIGN TABLE kafka_strict (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int,
    some_text text,
    some_date date,
    some_time timestamp
)
SERVER kafka_server OPTIONS
    (format 'csv', topic 'contrib_regress_junk', batch_size '30', buffer_delay '100', strict 'true');

SELECT * FROM kafka_strict WHERE offs>=0 and part=0;

CREATE FOREIGN TABLE kafka_ignore_junk (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int,
    some_text text,
    some_date date,
    some_time timestamp
)
SERVER kafka_server OPTIONS
    (format 'csv', topic 'contrib_regress_junk', batch_size '30', buffer_delay '100', ignore_junk 'true');

SELECT * FROM kafka_ignore_junk WHERE offs>=0 and part=0;

