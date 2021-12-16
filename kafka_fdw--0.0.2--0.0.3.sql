DROP FUNCTION kafka_get_watermarks(IN regclass, OUT int, OUT int, OUT int);

CREATE FUNCTION kafka_get_watermarks(
    IN rel regclass,
	OUT partition int,
	OUT offset_low bigint,
	OUT offset_high bigint)
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

ALTER TABLE kafka_fdw_offset_dump ALTER COLUMN "offset" TYPE bigint;
