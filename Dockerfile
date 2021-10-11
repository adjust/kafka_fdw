ARG PG_VERSION
FROM postgres:${PG_VERSION}-alpine

# Environment
ENV LANG=C.UTF-8 REPO=/repo

# Install dependencies
RUN apk --no-cache add make musl-dev gcc clang llvm util-linux-dev wget librdkafka-dev openjdk8-jre;

# Make postgres directories writable
RUN chmod a+rwx /usr/local/lib/postgresql && \
    chmod a+rwx /usr/local/lib/postgresql/bitcode && \
    chmod a+rwx /usr/local/share/postgresql/extension

# Make directories
RUN	mkdir -p $REPO && \
    mkdir /kafka && \
    chmod a+rwx /kafka

# Add repo
ADD . $REPO
RUN chown -R postgres:postgres $REPO
WORKDIR $REPO

USER postgres

# Expose zookeeper and kafka ports (may be useful for local debug)
EXPOSE 2181 9092

ENTRYPOINT ["/repo/test/run_tests.sh"]

