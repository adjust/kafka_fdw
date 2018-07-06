CREATE FOREIGN TABLE kafka_test_raw (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    message text
)
SERVER kafka_server OPTIONS
    (format 'raw', topic 'contrib_regress4', batch_size '30', buffer_delay '100');

-- we can only check if the meesaage has a certain pattern as part / offs is not deteministic
SELECT part, offs,  message ~ E'\\d{4},"It\'s some text, that is for number \\d{4}",\\d{4}-\\d{2}-\\d{2},\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}' FROM kafka_test_raw WHERE part=1 AND offs BETWEEN 100 AND 110 ORDER BY offs;

-- check writing raw data to kafka
INSERT INTO kafka_test_raw (message) VALUES ('123');
SELECT message FROM kafka_test_raw WHERE message = '123';