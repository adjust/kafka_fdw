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
