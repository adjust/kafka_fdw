# Kafka Foreign Data Wrapper for PostgreSQL

[![Build Status](https://travis-ci.org/adjust/kafka_fdw.svg?branch=master)](https://travis-ci.org/adjust/kafka_fdw)

At this point the project is not yet production ready.
Use with care. Pull requests welcome


A simple  foreign data wrapper for Kafka which allows it to be treated as
a table.

Currently kafka_fdw allows message parsing in csv and json format.
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
For more usage options see test/expected

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

The offset and partition columns are special.  Due to the way Kafka works, we _should_
specify these on all queries.


## Notes on Supported Formats

### CSV

CSV, like a PostgreSQL relation, represents data as a series of tuples.  In this respect
the mapping is fairly straight forward.  We use position to map to columns.  What CSV lacks'
however is any sort of schema enforcement between rows, to ensure that all values of a
particular column have the same data types, and other schema checks we expect from a relational
database.  For this reason, it is important to ask how much one trusts the schema enforcement
of the writers.  If the schema enforcement is trusted then you can assume that bad data should
throw an error.  But if it is not, then the error handling options documented here should be
used to enforce schema on read and skip but flag malformed rows.

On one side you can use `strict 'true'` if the format will never change and you fully trust
the writer to properly enforce schemas.  If you trust the writer to always be correct and allow
new columns to be added on to the end, however, you should leave this setting off.

If you do not trust the writer and wish to enforce schema on read only, then set a column with
the option junk 'true'` and another with the option `junk_error 'true'`.

## JSON

JSON has many of the same schema validation issues that CSV does but there are tools and standards
to validate and check JSON documents against schema specifications.  Thus the same error handling
recommendations that apply to CSV above apply here.

Mapping JSON fields to the relation fields is somewhat less straight forward than it with CSV.  JSON
objects represent key/value mappings in an arbitrary order.  For JSON we apply a mapping of the
tupple attribute name to the JSON object key name.  For JSON tables one uses the json option to specify
the json property mapped to.

The example in our test script is:

```
CREATE FOREIGN TABLE kafka_test_json (
    part int OPTIONS (partition 'true'),
    offs bigint OPTIONS (offset 'true'),
    some_int int OPTIONS (json 'int_val'),
    some_text text OPTIONS (json 'text_val'),
    some_date date OPTIONS (json 'date_val'),
    some_time timestamp OPTIONS (json 'time_val')
)

SERVER kafka_server OPTIONS
    (format 'json', topic 'contrib_regress_json', batch_size '30', buffer_delay '100');
```

Here you can see that a message on partition 2, with an offset of 53 containing the document:

```
{
   "text_val": "Some arbitrary text, apparently",
   "date_val": "2011-05-04",
   "int_val": 3,
   "time_val": "2011-04-14 22:22:22"
}
```

would be turned into

(2, 13, 3, "Some text, apparently", 2011-05-04, "2011-04-14 22:22:22")

as a row in the above table.

Currently the Kafka FDW does not support series of JSON arrays, only JSON objects.  JSON arrays
in objects can be presented as text or JSON/JSONB fields, however.


## Querying

With the defined meta columns you can query like so:

```
SELECT * FROM kafka_test WHERE part = 0 AND offs > 1000 LIMIT 60;
```

Here offs is the offset column. And defaults to  offset beginning.
Without any partition specified all partitions will be scanned.

Querying across partitions could be done as well.

```
SELECT * FROM kafka_test WHERE (part = 0 AND offs > 100) OR (part = 1 AND offs > 300) OR (part = 3 AND offs > 700)
```

## Error handling

The default for consuming kafka data is not very strict i.e. to less columns
will be assumed be NULL and to many will be ignored.
If you don't like this behaviour you can enable strictness via table options
`strict 'true'`. Thus any such column will error out the query.
However invalid or unparsable data e.g. text for numeric data or invalid date
or such will still error out per default. To ignore such data you can pass
`ignore_junk 'true'` as table options and these columns will be set to NULL.
Alternatively you can add table columns with the attributes
`junk 'true'` and / or `junk_error 'true'`. While fetching data kafka_fdw
will then put the whole payload into the junk column and / or the errormessage(s)
into the junk_error column.
see test/sql/junk_test.sql for a usage example.


## Producing

Inserting Data into kafak works with INSERT statements. If you provide the partition
as a values that will be user otherwise kafkas builtin partitioner will select partition.


add partition as a value

```
INSERT INTO kafka_test(part, some_int, some_text)
VALUES
    (0, 5464565, 'some text goes into partition 0'),
    (1, 5464565, 'some text goes into partition 1'),
    (0, 5464565, 'some text goes into partition 0'),
    (3, 5464565, 'some text goes into partition 3'),
    (NULL, 5464565, 'some text goes into partition selected by kafka');
```
use built in partitioner

```
INSERT INTO kafka_test(some_int, some_text)
VALUES
    (5464565, 'some text goes into partition selected by kafka');
```

### Testing

is currently broken I can't manage to have a proper repeatable topic setup

### Development

Although it works when used properly we need way more error handling.
Basically more test are needed for inapproiate usage like
no topic specified, topic doesn't exist, no partition and offsetcolumn defined
wrong format specification and stuff that might come.

### Future

The idea is to make the FDW more flexible in usage

* specify other formats like protobuf or binary

* specify encoding

* optimize performance with check_selective_binary_conversion
    i.e. WHEN just a single column is projected like
        SELECT one_coll FROM forein_table WHERE ...
    we won't need to take the effort to convert all columns

* better cost and row estmate

* some analyze options would be nice

* parallelism
    with multiple partitions we could theoretically consum them
    in parallel
....



