DROP FUNCTION kafka_get_watermarks(IN regclass, OUT int, OUT int, OUT int);

CREATE FUNCTION kafka_get_watermarks(
    IN rel regclass,
	OUT partition int,
	OUT offset_low bigint,
	OUT offset_high bigint)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'kafka_get_watermarks'
LANGUAGE C STRICT;

ALTER TABLE kafka_fdw_offset_dump ALTER COLUMN "offset" TYPE bigint;
