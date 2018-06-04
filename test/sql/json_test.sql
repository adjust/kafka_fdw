\i test/sql/setup.inc
-- standard setup
CREATE FOREIGN TABLE kafka_test_json (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int OPTIONS (json 'int_val'),
    some_text text OPTIONS (json 'text_val'),
    some_date date OPTIONS (json 'date_val'),
    some_time timestamp OPTIONS (json 'time_val')
)

SERVER kafka_server OPTIONS
    (format 'json', topic 'contrib_regress_json', batch_size '30', buffer_delay '100');

SELECT * FROM kafka_test_json WHERE part = 0 AND offs > 0 LIMIT 30;
SELECT * FROM kafka_test_json WHERE part = 0 AND offs > 10 LIMIT 30;
SELECT * FROM kafka_test_json WHERE part = 0 AND offs > 100 LIMIT 30;
SELECT * FROM kafka_test_json WHERE part = 0 AND offs > 1000 LIMIT 30;
SELECT * FROM kafka_test_json WHERE part = 0 AND offs > 10000 LIMIT 30;
SELECT * FROM kafka_test_json WHERE part = 0 AND offs > 90000 LIMIT 30;
SELECT * FROM kafka_test_json WHERE part = 0 AND offs > 100000 LIMIT 30;
