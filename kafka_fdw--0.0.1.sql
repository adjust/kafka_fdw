CREATE TABLE kafka_fdw_offset_dump(
    tbloid oid,
    partition int,
    "offset" int,
    last_fetch timestamp DEFAULT statement_timestamp(),
    PRIMARY KEY(tbloid, partition)
);
SELECT pg_catalog.pg_extension_config_dump('kafka_fdw_offset_dump', '');

CREATE FUNCTION kafka_fdw_handler()
RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FUNCTION kafka_fdw_validator(text[], oid)
RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE FOREIGN DATA WRAPPER kafka_fdw
  HANDLER kafka_fdw_handler
  VALIDATOR kafka_fdw_validator;

