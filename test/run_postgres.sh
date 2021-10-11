#!/usr/bin/env bash

set -e

initdb
pg_ctl -D /var/lib/postgresql/data -l /tmp/postgres.log start
