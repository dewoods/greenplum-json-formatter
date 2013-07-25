DROP FUNCTION IF EXISTS json_formatter_read();
CREATE FUNCTION json_formatter_read() RETURNS record
as '$libdir/json_formatter.so', 'json_formatter_read'
LANGUAGE C STABLE;

DROP FUNCTION IF EXISTS json_formatter_write(record);
CREATE FUNCTION json_formatter_write(record) RETURNS bytea
as '$libdir/json_formatter.so', 'json_formatter_write'
LANGUAGE C STABLE;
