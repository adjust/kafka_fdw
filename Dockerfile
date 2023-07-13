ARG PG_VERSION
FROM postgres:${PG_VERSION}-alpine AS base

# Environment
ENV LANG=C.UTF-8
ENV REPO=/repo
ENV KAFKA_PRODUCER="/kafka/bin/kafka-console-producer.sh"
ENV KAFKA_TOPICS="/kafka/bin/kafka-topics.sh"

# Make postgres directories writable
RUN chmod a+rwx /usr/local/lib/postgresql && \
    chmod a+rwx /usr/local/lib/postgresql/bitcode || true && \
    chmod a+rwx /usr/local/share/postgresql/extension && \
# Make directories
    mkdir -p $REPO && \
    mkdir /kafka && \
    chmod a+rwx /kafka

# Add repo
ADD . $REPO
RUN chown -R postgres:postgres $REPO
WORKDIR $REPO

# Expose zookeeper and kafka ports (may be useful for local debug)
EXPOSE 2181 9092

# clang-none target
FROM base AS clang-none

# Install dependencies
RUN apk --no-cache add make musl-dev gcc util-linux-dev wget librdkafka-dev openjdk8-jre;
USER postgres

ENTRYPOINT ["/repo/test/run_tests.sh"]

# clang-15 target
FROM base AS clang-15

# Install dependencies
RUN apk --no-cache add make musl-dev gcc clang15 llvm15 util-linux-dev wget librdkafka-dev openjdk8-jre;
USER postgres

ENTRYPOINT ["/repo/test/run_tests.sh"]
