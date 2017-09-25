
# https://www.cloudera.com/documentation/kafka/latest/topics/kafka_command_line.html

/* list topics */
kafka-topics --zookeeper localhost:2181 --list

/* create topic */
kafka-topics --zookeeper localhost:2181 --create --topic contrib_regress --partitions 4 --replication-factor 1

/* write some data */

cat test_data.csv | kafka-console-producer --broker-list localhost:9092 --topic contrib_regress

/* read data */
kafka-console-consumer --bootstrap-server localhost:9092 --topic contrib_regress --from-beginning

/* purge */
kafka-topics --zookeeper localhost:2181 --delete --topic contrib_regress