#!/bin/bash
: ${PG_PORT:=5432}

topic_part4="contrib_regress4"
simple_topic="contrib_regress"
out_sql="SELECT i, 'It''s some text, that is for number '||i, ('2015-01-01'::date + (i || ' seconds')::interval)::date, ('2015-01-01'::date + (i || ' seconds')::interval)::timestamp FROM generate_series(1,1e6::int, 10) i ORDER BY i"
kafka_cmd="kafka-console-producer --broker-list localhost:9092 --topic "

# delete topic if it might exist
for t in $simple_topic $topic_part4; do
    kafka-topics --zookeeper localhost:2181 --delete --topic ${t}
done
sleep 2

# create topic with 4 partitions
kafka-topics --zookeeper localhost:2181 --create --topic ${simple_topic} --partitions 1 --replication-factor 1
kafka-topics --zookeeper localhost:2181 --create --topic ${topic_part4} --partitions 4 --replication-factor 1

# write some test data to topicc
for t in $simple_topic $topic_part4; do
	psql -c "COPY(${out_sql}) TO STDOUT (FORMAT CSV);" -d postgres -U postgres -p $PG_PORT -o "| ${kafka_cmd} ${t}" >/dev/null
done;