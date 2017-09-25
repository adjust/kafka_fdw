#!/bin/bash
: ${PG_PORT:=5432}

topic="contrib_regress"
out_sql="SELECT i, 'It''s some text, that is for number '||i, ('2015-01-01'::date + (i || ' seconds')::interval)::date, ('2015-01-01'::date + (i || ' seconds')::interval)::timestamp FROM generate_series(1,1e6::int, 10) i"
kafka_cmd="kafka-console-producer --broker-list localhost:9092 --topic ${topic}"

# delete topic if it might exist
kafka-topics --zookeeper localhost:2181 --delete --topic ${topic}
sleep 10

# create topic with 4 partitions
kafka-topics --zookeeper localhost:2181 --create --topic ${topic} --partitions 4 --replication-factor 1

# write some test data to topic
psql -d postgres -U postgres -p $PG_PORT <<EOF
\copy (${out_sql}) TO PROGRAM '${kafka_cmd}' (FORMAT CSV);
EOF
