#!/usr/bin/env bash

set -e

test/run_kafka.sh
test/run_postgres.sh

set -x

# build extension
make install CFLAGS="${CFLAGS}"

# run regression tests
status=0
make installcheck PGUSER=postgres || status=$?

# show diff if needed
if [[ ${status} -ne 0 ]] && [[ -f regression.diffs ]]; then
    cat regression.diffs;
fi

exit ${status}

