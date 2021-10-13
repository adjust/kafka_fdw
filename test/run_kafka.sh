#!/bin/bash
#
# This script installs and runs Kafka instance on Debian like distributions.
# Dedicated to be run by root in Docker containers.
#
set -ex

KAFKA_VERSION=2.8.1
KAFKA_ARCHIVE=kafka_2.13-${KAFKA_VERSION}.tgz

# Install JRE
mkdir -p /usr/share/man/man1
apt-get update
export DEBIAN_FRONTEND=noninteractive
apt-get install -yq default-jre-headless

# Download Apache Kafka
apt-get install -yq wget
wget https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}
mkdir /kafka
tar -xzf ${KAFKA_ARCHIVE} -C /kafka --strip-components=1
export PATH="/kafka/bin/:$PATH"

# Start Zookeeper and Kafka
zookeeper-server-start.sh /kafka/config/zookeeper.properties > /tmp/zookeeper.log &
kafka-server-start.sh /kafka/config/server.properties > /tmp/kafka.log &
