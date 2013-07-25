greenplum-json-formatter
========================

Greenplum extension for reading and writing JSON data

Installation
------------
greenplum-json-formatter has been tested on the following operating systems:
- OSX
- Linux

Build using included Makefile:

    $ make
    $ make install
  
Note that in a clustered environment the shared object must be copied to all segments using a command similar to the following:

    $ gpscp -f ~/gpconfigs/hostfile lib/json_formatter.so =:/usr/local/greenplum-db/lib/postgresql/json_formatter.so

Start gpfdist and run included unit tests if desired:

    $ sh test/gpfdist.sh
    $ make test
    
Note that some tests may fail in a clustered environment due to results being returned in a different order.  Also note that test/test.ddl.sql may need to be modified to contain the correct master hostname for your environment.
    
###Dependencies

####Required
-   [Jansson](http://www.digip.org/jansson/) - a C library for encoding, decoding and manipulating JSON data

Jansson libraries must be present on the Greenplum master, standby master, and all segment hosts.  Make sure libjansson is available in gpadmin's LD_LIBRARY_PATH or equivalent

####Optional
-   [Roundup](https://github.com/bmizerany/roundup) - a shell based unit testing tool

Required for running included unit tests

Usage
-----

###Reading JSON Data

Create a readable external table pointed to your JSON data source that uses the json_formatter_read custom formatter.  Note that the column names in your external table must match the JSON object names in your data file.

    CREATE EXTERNAL TABLE json_basic (
        id int
    ) LOCATION (
        'gpfdist://localhost:8081/data/basic.dat'
    ) FORMAT 'custom' (
        formatter=json_formatter_read
    );

Issue a SQL statement to read from your external table:

    # SELECT * FROM json_basic;

Assume the following input data:

    { "id": 1 },
    { "id": 2 },
    { "id": 3 }

You should expect the following result:

    # select * from json_basic;
     id 
    ----
      1
      2
      3
    (3 rows)

###Writing JSON Data

Create a writable external table pointed to your desired output file using the 'json_formatter_write' custom protocol:

    CREATE WRITABLE EXTERNAL TABLE out_basic (
        id int
    ) LOCATION (
        'gpfdist://localhost:8081/out/basic.dat'
    ) FORMAT 'custom' (
        formatter=json_formatter_write
    );

Insert data into your writable external table:

    # INSERT INTO out_basic SELECT generate_series(0,2);

JSON data will be written to your output file in parallel:

    $ cat out/basic.dat 
    {"id": 0}
    {"id": 1}
    {"id": 2}

###Nested JSON Data

Use the '.' delimiter in database column names to signify a nested JSON object:

    CREATE EXTERNAL TABLE nested (
        id int,
        "sub.subid" int,
        "sub.subsub.subsubid" int
    ) LOCATION (
        'gpfdist://localhost:8081/data/nested.dat'
    ) FORMAT 'custom' (
        formatter=json_formatter_read
    );

    { "id": 1, "sub": { "subid": 10, "subsub": { "subsubid": 100 } } }

    # select * from nested;
     id | sub.subid | sub.subsub.subsubid 
    ----+-----------+---------------------
      1 |        10 |                 100
    (1 rows)

The same will also work in reverse for a writable external table.

Limitations
-----------

The following known limitations will be addressed in a future release:
- No support for JSON arrays
- Cannot change nested object delimiter character
