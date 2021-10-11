#!/bin/bash
#
# This script installs and runs Kafka instance on Debian like distributions.
# Dedicated to be run by root in Docker containers.
#
set -ex

KAFKA_VERSION=2.8.1
KAFKA_ARCHIVE=kafka_2.13-${KAFKA_VERSION}.tgz

DIST=$(cat /etc/os-release | grep ^ID= | sed s/ID=//)

echo

if [ $DIST != 'alpine' ]; then
    # Install JRE
    mkdir -p /usr/share/man/man1
    apt-get update
    export DEBIAN_FRONTEND=noninteractive
    apt-get install -yq default-jre-headless wget
fi

# Download Apache Kafka
wget https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}
tar -xzf ${KAFKA_ARCHIVE} -C /kafka --strip-components=1
export PATH="/kafka/bin/:$PATH"

# Configuration
echo "advertised.listeners=PLAINTEXT://localhost:9092" >> /kafka/config/server.properties

# Start Zookeeper and Kafka
zookeeper-server-start.sh /kafka/config/zookeeper.properties > /tmp/zookeeper.log &
kafka-server-start.sh /kafka/config/server.properties > /tmp/kafka.log &
