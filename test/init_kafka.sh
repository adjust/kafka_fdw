#!/bin/bash
: ${KAFKA_PRODUCER:="/usr/local/bin/kafka-console-producer"}
: ${KAFKA_TOPICS:="/usr/local/bin/kafka-topics"}

topics=( contrib_regress4 contrib_regress contrib_regress_prod contrib_regress_prod_json contrib_regress_junk contrib_regress_json contrib_regress_json_junk )
partitions=( 4 1 4 4 1 1 1 )

if [[ -n "${PG_CONFIG}" ]]; then
	BIN=$(${PG_CONFIG} --bindir)
else
	BIN=$(pg_config --bindir)
fi

declare -a toppart
index=0

for t in "${topics[@]}"; do
  toppart[$index]="--topic ${t} --partitions ${partitions[${index}]}"
  ((index++))
done

topic_part4="contrib_regress4"
simple_topic="contrib_regress"
prod_topic="contrib_regress_prod"
json_prod_topic="contrib_regress_prod_json"
junk_topic="contrib_regress_junk"
json_topic="contrib_regress_json"
json_junk_topic="contrib_regress_json_junk"

out_sql="SELECT i as int_val, 'It''s some text, that is for number '||i as text_val, ('2015-01-01'::date + (i || ' seconds')::interval)::date as date_val, ('2015-01-01'::date + (i || ' seconds')::interval)::timestamp as time_val FROM generate_series(1,1e6::int, 10) i ORDER BY i"
kafka_cmd="$KAFKA_PRODUCER --broker-list localhost:9092 --topic"

# delete topic if it might exist
for t in "${topics[@]}"; do $KAFKA_TOPICS --zookeeper localhost:2181 --delete --topic ${t} & done; wait


# create topics with partitions
for t in "${toppart[@]}"; do $KAFKA_TOPICS --zookeeper localhost:2181 --create ${t} --replication-factor 1 & done; wait

# write some test data to topicc
$BIN/psql -c "COPY(SELECT json_build_object('int_val',int_val, 'text_val',text_val, 'date_val',date_val, 'time_val', time_val ) FROM (${out_sql}) t) TO STDOUT (FORMAT TEXT);" -d postgres -o "| ${kafka_cmd} ${json_topic}" >/dev/null &

for t in contrib_regress contrib_regress4; do $BIN/psql -c "COPY(${out_sql}) TO STDOUT (FORMAT CSV);" -d postgres -o "| ${kafka_cmd} ${t}" >/dev/null & done; wait


$kafka_cmd contrib_regress_junk <<-EOF
91,"correct line",01-01-2015,Thu Jan 01 01:31:00 2015
131,"additional data",01-01-2015,Thu Jan 01 02:11:00 2015,aditional data
161,"additional data although null",01-01-2015,Thu Jan 01 02:41:00 2015,
301,"correct line although last line null",01-01-2015,
371,"invalidat date",invalid_date,Thu Jan 01 06:11:00 2015
401,"unterminated string","01-01-2015,Thu Jan 01 06:41:00 2015
421,"correct line",01-01-2015,Thu Jan 01 07:01:00 2015
999999999999999,"invalid number",01-01-2015,Thu Jan 01 07:11:00 2015
521,"correct line",01-01-2015,Thu Jan 01 08:41:00 2015
999999999999999,"invalid number, invalid date and extra data",20-20-2015,Thu Jan 01 09:31:00 2015,extra data
"401,unterminated string,01-01-2015,Thu Jan 01 06:41:00 2015
EOF


$kafka_cmd contrib_regress_json_junk <<-EOF
{"int_val" : 999741, "text_val" : "correct line", "date_val" : "2015-01-12", "time_val" : "2015-01-12T13:42:21"}
{"int_val" : 999751, "text_val" : "additional data", "date_val" : "2015-01-12", "time_val" : "2015-01-12T13:42:31", "more_val": "to much data"}
{"int_val" : 999761, "text_val" : "additional data although null", "date_val" : "2015-01-12", "time_val" : "2015-01-12T13:42:41", "more_val": null}
{"int_val" : 999781, "text_val" : "invalidat date", "date_val" : "foob", "time_val" : "2015-01-12T13:43:01", "time_val": "2015-01-12T13:42:51"}
{"int_val" : 999791, "text_val" : "invalid json (unterminated quote)", "date_val" : "2015-01-12", "time_val : "2015-01-12T13:43:11"}
{"int_val" : 999801, "text_val" : "correct line", "date_val" : "2015-01-12", "time_val" : "2015-01-12T13:43:21"}
{"int_val" : 9998119999999999, "text_val" : "invalid number", "date_val" : "2015-01-12", "time_val" : "2015-01-12T13:43:31"}
{"int_val" : 999821, "text_val" : "correct line", "date_val" : "2015-01-12", "time_val" : "2015-01-12T13:43:41"}
{"int_val" : "9998119999999999",  "text_val" : "invalid number, invalid date and extra data", "date_val" : "2015-13-13", "time_val" : "2015-01-12T13:43:51", "foo": "just to much"}
{"int_val" : 999841, "text_val" : "empty time" , "date_val" : "2015-01-12", "time_val" : ""}
{"int_val" : 999851, "text_val" : "correct line null time", "date_val" : "2015-01-12", "time_val" : null}
{"int_val" : 999871, "invalid json no time" : "invalid json", "date_val" : "2015-01-12", "time_val" : }
EOF
