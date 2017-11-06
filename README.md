# Kafka Foreign Data Wrapper for PostgreSQL

[![Build Status](https://travis-ci.org/adjust/kafka_fdw.svg?branch=master)](https://travis-ci.org/adjust/kafka_fdw)

At this point the project is not yet production ready.
Use with care. Pull requests welcome


A simple  foreign data wrapper for Kafka which allows it to be treated as
a table.

Currently kafka_fdw allows message parsing in csv format only.
More might come in a future release.


## Build

The FDW uses the librdkafka C client library. https://github.com/edenhill/librdkafka
to build against installed librdkafka and postgres run
`make && make install`

to run test

`make installcheck`

not this runs an integration test against an asumed running
kafaka on localhost:9092 with zookeeper on  localhost:2181
see `test/init_kafka.sh`


## Usage

CREATE SERVER must specifie a brokerlist using option `brokers`
```SQL
CREATE SERVER kafka_server
FOREIGN DATA WRAPPER kafka_fdw
OPTIONS (brokers 'localhost:9092');
```

CREATE USER MAPPING
```SQL
CREATE USER MAPPING FOR PUBLIC SERVER kafka_server;
```

CREATE FOREIGN TABLE
must specify the two meta columns for partition and offset.
These can be named abritrary just must be specified wich is what using options.
Note offset is a sql reserved keyword so naming a column `offset` needs quotation
when used.
The remaining columns must match the expected csv message format.

```
CREATE FOREIGN TABLE kafka_test (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int,
    some_text text,
    some_date date,
    some_time timestamp
)
SERVER kafka_server OPTIONS
    (format 'csv', topic 'contrib_regress', batch_size '30', buffer_delay '100');
```

## Querying

With the defined meta columns you can query like so:

```
SELECT * FROM kafka_test WHERE part = 0 AND offs > 1000 LIMIT 60;
```


### Testing

is currently broken I can't manage to have a proper repeatable topic setup

### Development

Although it works when used properly we need way more error handling.
Basically more test are needed for inaproiate usage like
no topic specified, topic doesn't exist, no partition and offsetcolumn defined
wrong format specification and stuff that might come.

### Future

The idea is to make the FDW more flexible in usage

* specify multiple offset, partition tuples in WHERE clause
    i.e. `WHERE (part,off) IN((0,100), (1,500), (2,300))`
    in `kafka_expr.c`
    or no partition at all using all of them
    (`connection.c` Fetch metadata)

* specify other formats like json or even binary

* specify encoding

* optimize performance with check_selective_binary_conversion
    i.e. WHEN just a single column is projected like
        SELECT one_coll FROM forein_table WHERE ...
    we won't need to take the effort to convert all columns

* better cost and row estmate

* some analyze options would be nice

* parallelizim
    once we allow multiple partitions we could theoretically consum them
    in parallel

....



