DROP SCHEMA IF EXISTS __json_formatter_test CASCADE;
CREATE SCHEMA __json_formatter_test;

SET search_path TO __json_formatter_test;

DROP EXTERNAL TABLE IF EXISTS basic;
CREATE EXTERNAL TABLE basic (
    id int
) LOCATION (
    'gpfdist://localhost:8081/data/basic.dat'
) FORMAT 'custom' (
    formatter=json_formatter_read
);

DROP EXTERNAL TABLE IF EXISTS types;
CREATE EXTERNAL TABLE types (
    id int,
    i2 int2,
    i4 int4,
    i8 int8,
    t text,
    v varchar(255),
    ts text,
    f4 float4,
    f8 float8
) LOCATION (
    'gpfdist://localhost:8081/data/types.dat'
) FORMAT 'custom' (
    formatter=json_formatter_read
);

DROP EXTERNAL TABLE IF EXISTS error1;
CREATE EXTERNAL TABLE error1 (
    id int,
    t text,
    f4 float4,
    f8 float8
) LOCATION (
    'gpfdist://localhost:8081/data/error1.dat'
) FORMAT 'custom' (
    formatter=json_formatter_read
) LOG ERRORS INTO error1_err SEGMENT REJECT LIMIT 25 ROWS;

DROP EXTERNAL TABLE IF EXISTS twitter1;
CREATE EXTERNAL TABLE twitter1 (
    id bigint,
    created_at text,
    "text" text
) LOCATION (
    'gpfdist://localhost:8081/data/twitter.json.1'
) FORMAT 'custom' (
    formatter=json_formatter_read
);

DROP EXTERNAL TABLE IF EXISTS twitter100;
CREATE EXTERNAL TABLE twitter100 (
    id bigint,
    created_at text,
    "user.name" text,
    "user.id" bigint,
    "user.friends_count" int,
    "text" text
) LOCATION (
    'gpfdist://localhost:8081/data/twitter.json.100'
) FORMAT 'custom' (
    formatter=json_formatter_read
) LOG ERRORS INTO twitter100_err SEGMENT REJECT LIMIT 25 ROWS;

DROP EXTERNAL TABLE IF EXISTS twitter1000;
CREATE EXTERNAL TABLE twitter1000 (
    id bigint,
    created_at text,
    "text" text
) LOCATION (
    'gpfdist://localhost:8081/data/twitter.json'
) FORMAT 'custom' (
    formatter=json_formatter_read
) LOG ERRORS INTO twitter1000_err SEGMENT REJECT LIMIT 250 ROWS;

DROP EXTERNAL TABLE IF EXISTS nested;
CREATE EXTERNAL TABLE nested (
    id int,
    "sub.subid" int,
    "sub.subsub.subsubid" int
) LOCATION (
    'gpfdist://localhost:8081/data/nested.dat'
) FORMAT 'custom' (
    formatter=json_formatter_read
);

DROP EXTERNAL TABLE IF EXISTS out_basic;
CREATE WRITABLE EXTERNAL TABLE out_basic (
    id int
) LOCATION (
    'gpfdist://localhost:8081/out/basic.dat'
) FORMAT 'custom' (
    formatter=json_formatter_write
);

DROP EXTERNAL TABLE IF EXISTS out_types;
CREATE WRITABLE EXTERNAL TABLE out_types (
    id int,
    i2 int2,
    i4 int4,
    i8 int8,
    t text,
    v varchar(255),
    ts text,
    f4 float4,
    f8 float8
) LOCATION (
    'gpfdist://localhost:8081/out/types.dat'
) FORMAT 'custom' (
    formatter=json_formatter_write
);

DROP EXTERNAL TABLE IF EXISTS out_nested;
CREATE WRITABLE EXTERNAL TABLE out_nested (
    id int,
    "sub.subid" int,
    "sub.subsub.subsubid" int
) LOCATION (
    'gpfdist://localhost:8081/out/nested.dat'
) FORMAT 'custom' (
    formatter=json_formatter_write
);

DROP EXTERNAL TABLE IF EXISTS out_twitter;
CREATE WRITABLE EXTERNAL TABLE out_twitter (
    id bigint,
    created_at text,
    "user.name" text,
    "user.id" bigint,
    "user.friends_count" int,
    "text" text
) LOCATION (
    'gpfdist://localhost:8081/out/twitter.json'
) FORMAT 'custom' (
    formatter=json_formatter_write
);
