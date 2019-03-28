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

CREATE FUNCTION kafka_get_watermarks(IN rel regclass,
	OUT partition int,
	OUT offset_low int,
	OUT offset_high int)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'kafka_get_watermarks'
LANGUAGE C STRICT;

DO $$
DECLARE version_num INTEGER;
BEGIN
    SELECT current_setting('server_version_num') INTO STRICT version_num;
    IF version_num > 90600 THEN
        EXECUTE 'ALTER FUNCTION kafka_get_watermarks(regclass) PARALLEL SAFE';
    END IF;
END
$$;
