\i test/sql/setup.inc
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

SELECT * FROM kafka_strict WHERE offs=0 and part=0 LIMIT 1;
SELECT * FROM kafka_strict WHERE offs=1 and part=0 LIMIT 1;
SELECT * FROM kafka_strict WHERE offs=2 and part=0 LIMIT 1;
SELECT * FROM kafka_strict WHERE offs=3 and part=0 LIMIT 1;
SELECT * FROM kafka_strict WHERE offs=4 and part=0 LIMIT 1;
SELECT * FROM kafka_strict WHERE offs=5 and part=0 LIMIT 1;
SELECT * FROM kafka_strict WHERE offs=6 and part=0 LIMIT 1;
SELECT * FROM kafka_strict WHERE offs=8 and part=0 LIMIT 1;
SELECT * FROM kafka_strict WHERE offs=9 and part=0 LIMIT 1;
SELECT * FROM kafka_strict WHERE offs=10 and part=0 LIMIT 1;

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


CREATE FOREIGN TABLE kafka_strict_json (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int OPTIONS (json 'int_val'),
    text_val text,
    some_date date OPTIONS (json 'date_val'),
    time_val timestamp
)
SERVER kafka_server OPTIONS
    (format 'json', topic 'contrib_regress_json_junk', batch_size '30', buffer_delay '100', strict 'true');

SELECT * FROM kafka_strict_json WHERE offs=0 and part=0 LIMIT 1;
SELECT * FROM kafka_strict_json WHERE offs=1 and part=0 LIMIT 1;
SELECT * FROM kafka_strict_json WHERE offs=2 and part=0 LIMIT 1;
SELECT * FROM kafka_strict_json WHERE offs=3 and part=0 LIMIT 1;
SELECT * FROM kafka_strict_json WHERE offs=4 and part=0 LIMIT 1;
SELECT * FROM kafka_strict_json WHERE offs=5 and part=0 LIMIT 1;
SELECT * FROM kafka_strict_json WHERE offs=6 and part=0 LIMIT 1;
SELECT * FROM kafka_strict_json WHERE offs=8 and part=0 LIMIT 1;
SELECT * FROM kafka_strict_json WHERE offs=9 and part=0 LIMIT 1;
SELECT * FROM kafka_strict_json WHERE offs=10 and part=0 LIMIT 1;
SELECT * FROM kafka_strict_json WHERE offs=11 and part=0 LIMIT 1;

CREATE FOREIGN TABLE kafka_ignore_junk_json (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int OPTIONS (json 'int_val'),
    text_val text,
    some_date date OPTIONS (json 'date_val'),
    time_val timestamp
)
SERVER kafka_server OPTIONS
    (format 'json', topic 'contrib_regress_json_junk', batch_size '30', buffer_delay '100', ignore_junk 'true');

SELECT * FROM kafka_ignore_junk_json WHERE offs>=0 and part=0;

