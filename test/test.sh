#!/usr/bin/env roundup

describe "myprot"

SCHEMA_NAME=__json_formatter_test

before() {
    count=`ls -1 test/out/* 2>/dev/null | wc -l`
    if [ $count != 0 ]
    then
        rm test/out/*
    fi
    psql -f test/test.ddl.sql 2> /dev/null
}

after() {
    #psql -f test/teardown.sql 2> /dev/null
    true
}

it_in_sanity() {
    psql -tA -c "select * from $SCHEMA_NAME.basic" | diff - test/expected/basic.out
}

it_in_types() {
    psql -tA -c "select * from $SCHEMA_NAME.types" | diff - test/expected/types.out
}

it_in_error1() {
    psql -tA -c "select * from $SCHEMA_NAME.error1" 2>&1 | diff - test/expected/error1.out
    psql -tA -c "select linenum, errmsg, rawbytes from $SCHEMA_NAME.error1_err" 2>&1 | diff - test/expected/error1_err.out
}

it_in_nested() {
    psql -tA -c "select * from $SCHEMA_NAME.nested" | diff - test/expected/nested.out
}

it_in_twitter1() {
    psql -tA -c "select * from $SCHEMA_NAME.twitter1" | diff - test/expected/twitter1.out
}

it_in_twitter100() {
    psql -tA -c "select * from $SCHEMA_NAME.twitter100" 2>&1 | diff - test/expected/twitter100.out
    psql -tA -c "select linenum, errmsg, rawbytes from $SCHEMA_NAME.twitter100_err" 2>&1 | diff - test/expected/twitter100_err.out
}

it_in_twitter1000() {
    psql -tA -c "select * from $SCHEMA_NAME.twitter1000" 2>&1 | diff - test/expected/twitter1000.out
    psql -tA -c "select linenum, errmsg, rawbytes from $SCHEMA_NAME.twitter1000_err" 2>&1 | diff - test/expected/twitter1000_err.out
}

it_out_sanity() {
    psql -tA -c "insert into $SCHEMA_NAME.out_basic select generate_series(0,3)"
    diff test/out/basic.dat test/expected/out_basic.dat
}

it_out_types() {
    psql -tA -c "insert into $SCHEMA_NAME.out_types select 1, 2::int2, 3::int4, 4::int8, 'text'::text, 'varchar'::varchar, 'timestamp', 4.4::float4, 8.8::float8"
    diff test/out/types.dat test/expected/out_types.dat
}

it_out_nested() {
    psql -tA -c "insert into $SCHEMA_NAME.out_nested select 1, 2, 3"
    diff test/out/nested.dat test/expected/out_nested.dat
}

it_out_twitter() {
    psql -tA -c "insert into $SCHEMA_NAME.out_twitter select * from $SCHEMA_NAME.twitter100 order by id"
    #diff test/out/twitter.json test/expected/out_twitter100.json
    sort test/out/twitter.json | diff - test/expected/out_twitter100.json
}
