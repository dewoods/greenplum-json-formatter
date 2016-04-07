/*
 * Copyright (c) 2013 Dillon Woods <dewoods@gmail.com>
 *
 * greenplum-json-formatter is free software; you can redistribute it and/or modify
 * it under the terms of the MIT license. See LICENSE for details.
 */

#include <string.h>
#include "jansson.h"

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"

#include "access/formatter.h"
#include "catalog/pg_proc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/memutils.h"
#include "utils/elog.h"

PG_MODULE_MAGIC;
PG_FUNCTION_INFO_V1( json_formatter_read );
PG_FUNCTION_INFO_V1( json_formatter_write );

Datum json_formatter_read( PG_FUNCTION_ARGS );
Datum json_formatter_write( PG_FUNCTION_ARGS );

typedef struct {
    int             ncols;
    Datum           *values;
    bool            *nulls;
    char            *j_buf;
    json_t          *j_root;
    json_error_t    *j_error;
    int             j_len;
    int             j_counter;
    int             j_cursor;
    int             rownum;
} user_read_ctx_t;

typedef struct {
    int             ncolumns;
    json_t          *j_root;
    json_t          **j_vals;
    bytea           *buf;
    Datum           *dbvalues;
    bool            *dbnulls;
} user_write_ctx_t;

Datum
json_formatter_read( PG_FUNCTION_ARGS ) {
    HeapTuple       tuple;
    TupleDesc       tupdesc;
    MemoryContext   mc, omc;
    user_read_ctx_t      *user_ctx;
    char            *data_buf;
    int             data_cur;
    int             data_len;
    int             ncols = 0;
    int             i=0;

    if( !CALLED_AS_FORMATTER( fcinfo ) )
        elog( ERROR, "json_formatter_read: not called by format manager" );

    tupdesc = FORMATTER_GET_TUPDESC( fcinfo );
    ncols = tupdesc->natts;
    user_ctx = (user_read_ctx_t *)FORMATTER_GET_USER_CTX( fcinfo );

    /**
     * Get input data
     */
    data_buf = FORMATTER_GET_DATABUF( fcinfo );
    data_len = FORMATTER_GET_DATALEN( fcinfo );
    data_cur = FORMATTER_GET_DATACURSOR( fcinfo );

    /**
     * First call to formatter, setup context
     */
    if( user_ctx == NULL ) {
        user_ctx = palloc( sizeof( user_read_ctx_t ) );
        user_ctx->ncols = ncols;
        user_ctx->values = palloc( sizeof(Datum) * ncols );
        user_ctx->nulls = palloc( sizeof(bool) * ncols );
        user_ctx->j_buf = palloc( sizeof(char) * data_len );
        user_ctx->j_root = palloc( sizeof(json_t) );
        user_ctx->j_error = palloc( sizeof(json_error_t) );
        user_ctx->j_len = 0;
        user_ctx->j_counter = 0;
        user_ctx->j_cursor = 0;
        user_ctx->rownum = 0;


        FORMATTER_SET_USER_CTX( fcinfo, user_ctx );
    } else {
        user_ctx->rownum++;
    }

    /**
     * Clear column buffers
     */
    MemSet( user_ctx->values, 0, ncols * sizeof(Datum) );
    MemSet( user_ctx->nulls, false, ncols * sizeof(bool) );

    /**
     * Switch memory contexts, create tuple from data
     */
    mc = FORMATTER_GET_PER_ROW_MEM_CTX( fcinfo );
    omc = MemoryContextSwitchTo( mc );

    char c;
    int length = 0;

    //elog( NOTICE, "data buffer -> ncols: %d, len: %d, cur: %d - %hhd", ncols, data_len, data_cur, data_buf[data_cur] );

    if( data_cur == data_len ) {
        MemoryContextSwitchTo( omc );
        FORMATTER_RETURN_NOTIFICATION( fcinfo, FMT_NEED_MORE_DATA );
    }

    /**
     * Scan to beginning of JSON object
     */
    while( user_ctx->j_counter == 0 ) {
        if( data_cur == data_len ) {
            FORMATTER_SET_DATACURSOR( fcinfo, data_cur );
            MemoryContextSwitchTo( omc );
            FORMATTER_RETURN_NOTIFICATION( fcinfo, FMT_NEED_MORE_DATA );
        }

        if( data_buf[data_cur] == ' '
            || data_buf[data_cur] == ','
            || data_buf[data_cur] == '\n'
            || data_buf[data_cur] == '\r'
        ) {
            FORMATTER_SET_DATACURSOR( fcinfo, ++data_cur );
            continue;
        }

        if( data_buf[data_cur] != '{' ) {
            elog( ERROR, "Invalid JSON Format, expected '{' found '%c'", data_buf[data_cur] );
        }

        user_ctx->j_counter++;
        length++;
    }

    /**
     * Scan to end of JSON object, match closing bracket
     */
    while( user_ctx->j_counter > 0 ) {
        /**
         * Make sure we have more data to scan
         */
        if( data_cur + length == data_len ) {
            MemoryContextSwitchTo( omc );

            if( FORMATTER_GET_SAW_EOF( fcinfo ) ) {
                FORMATTER_SET_BAD_ROW_NUM( fcinfo, user_ctx->rownum );
                FORMATTER_SET_BAD_ROW_DATA( fcinfo, data_buf+data_cur, length );
                ereport( ERROR, (
                    errcode( ERRCODE_DATA_EXCEPTION ),
                    errmsg( "Invalid JSON object" )
                ) );
            } else {
                FORMATTER_RETURN_NOTIFICATION( fcinfo, FMT_NEED_MORE_DATA );
            }
        }

        c = data_buf[data_cur + length];

        if( c == '{' )
            user_ctx->j_counter++;
        else if( c == '}' )
            user_ctx->j_counter--;

        length++;
    }

    user_ctx->j_len = length;
    memcpy( user_ctx->j_buf, data_buf+data_cur, user_ctx->j_len );

    //elog( NOTICE, "Complete str: %d:%s", user_ctx->j_len, user_ctx->j_buf+4600 );

    user_ctx->j_root = json_loadb( user_ctx->j_buf, user_ctx->j_len, 0, user_ctx->j_error );
    if( !user_ctx->j_root ) {
        elog( ERROR, "Could not parse JSON string" );
    }
    if( !json_is_object(user_ctx->j_root) ) {
        elog( ERROR, "Could not parse JSON object" );
    }

    data_cur += user_ctx->j_len;
    user_ctx->j_counter = 0;

    /**
     * Pull each database column from the JSON object
     */
    for( i=0; i < ncols; i++ ) {
        Oid         type    = tupdesc->attrs[i]->atttypid;
        json_t      *val;
        char        *dbcolname, *tofree;
        char        *jobjname;

        /**
         * copy the name of the current database column, to be passed to strsep(3)
         */
        tofree = dbcolname = strdup( tupdesc->attrs[i]->attname.data );

        /**
         * Initially set to the root of the current JSON object
         */
        val = palloc( sizeof(json_t) );
        val = user_ctx->j_root;

        /**
         * If the db col name contains periods, traverse the JSON object to find the correct sub-element
         */
        bool this_obj_is_null = false;

        while( (jobjname = strsep( &dbcolname, "." )) != NULL ) {
            val = json_object_get( val, jobjname );
            if( !val ) {
              val = json_object();
              this_obj_is_null = true;
            }
        }
        if ( this_obj_is_null ) {
          val = json_null();
        }
        free( tofree );

        /**
         * We have the correct JSON object, now extract the expected database type
         */
        switch( type ) {
            case INT2OID:
            case INT4OID:
            case INT8OID:
            {
                if( json_is_null(val) ){
                  user_ctx->nulls[i] = true;
                } else if( !json_is_integer( val ) ) {
                    MemoryContextSwitchTo( omc );
                    FORMATTER_SET_BAD_ROW_NUM( fcinfo, user_ctx->rownum );
                    FORMATTER_SET_BAD_ROW_DATA( fcinfo, user_ctx->j_buf, user_ctx->j_len );
                    ereport( ERROR, (
                        errcode( ERRCODE_DATA_EXCEPTION ),
                        errmsg( "Wrong data type for column '%s', expected number", tupdesc->attrs[i]->attname.data )
                    ) );
                } else {
                  user_ctx->values[i] = (Datum)json_integer_value( val );
                  user_ctx->nulls[i] = false;
                }
                break;
            }
            case FLOAT4OID:
                if( json_is_null(val) ) {
                  user_ctx->nulls[i] = true;
                } else if( !json_is_real( val ) ) {
                    MemoryContextSwitchTo( omc );
                    FORMATTER_SET_BAD_ROW_NUM( fcinfo, user_ctx->rownum );
                    FORMATTER_SET_BAD_ROW_DATA( fcinfo, user_ctx->j_buf, user_ctx->j_len );
                    ereport( ERROR, (
                        errcode( ERRCODE_DATA_EXCEPTION ),
                        errmsg( "Wrong data type for column '%s', expected float4", tupdesc->attrs[i]->attname.data )
                    ) );
                } else {
                  user_ctx->values[i] = Float4GetDatum( json_real_value( val ) );
                  user_ctx->nulls[i] = false;
                }

                break;
            case FLOAT8OID:
            {
                if( json_is_null(val) ) {
                  user_ctx->nulls[i] = true;
                } else if( !json_is_real( val ) ) {
                    MemoryContextSwitchTo( omc );
                    FORMATTER_SET_BAD_ROW_NUM( fcinfo, user_ctx->rownum );
                    FORMATTER_SET_BAD_ROW_DATA( fcinfo, user_ctx->j_buf, user_ctx->j_len );
                    ereport( ERROR, (
                        errcode( ERRCODE_DATA_EXCEPTION ),
                        errmsg( "Wrong data type for column '%s', expected float8", tupdesc->attrs[i]->attname.data )
                    ) );
                } else {
                  user_ctx->values[i] = Float8GetDatum( json_real_value( val ) );
                  user_ctx->nulls[i] = false;
                }

                break;
            }
            case TEXTOID:
            case VARCHAROID:
            {
                const char  *strval;
                text        *txtval;
                if( json_is_null(val) ) {
                  user_ctx->nulls[i] = true;
                } else {

                  switch (json_typeof(val)) {
                    case JSON_STRING:
                      strval = json_string_value( val );
                    break;

                    default:
                      strval = json_dumps( val , 0);
                    break;
                  }


                  txtval = palloc( strlen( strval ) + VARHDRSZ );
                  SET_VARSIZE( txtval, strlen(strval) + VARHDRSZ );
                  memcpy( VARDATA(txtval), strval, strlen(strval) );

                  user_ctx->values[i] = PointerGetDatum( txtval );
                  user_ctx->nulls[i] = false;
                }
                break;
            }
            case BOOLOID:
            {
              if( json_is_null(val) ){
                user_ctx->nulls[i] = true;
              } else if( !json_is_boolean( val ) ) {
                  MemoryContextSwitchTo( omc );
                  FORMATTER_SET_BAD_ROW_NUM( fcinfo, user_ctx->rownum );
                  FORMATTER_SET_BAD_ROW_DATA( fcinfo, user_ctx->j_buf, user_ctx->j_len );
                  ereport( ERROR, (
                      errcode( ERRCODE_DATA_EXCEPTION ),
                      errmsg( "Wrong data type for column '%s', expected boolean", tupdesc->attrs[i]->attname.data )
                  ) );
              } else {
                user_ctx->values[i] = json_is_true( val );
                user_ctx->nulls[i] = false;
              }
              break;
            }
            default:
            {
                MemoryContextSwitchTo( omc );
                FORMATTER_SET_BAD_ROW_NUM( fcinfo, user_ctx->rownum );
                FORMATTER_SET_BAD_ROW_DATA( fcinfo, user_ctx->j_buf, user_ctx->j_len );
                ereport( ERROR, (
                    errcode( ERRCODE_DATA_EXCEPTION ),
                    errmsg( "Unsupported data type '%d' for column '%s'", type, tupdesc->attrs[i]->attname.data )
                ) );
            }
        }
    }

    MemoryContextSwitchTo( omc );
    FORMATTER_SET_DATACURSOR( fcinfo, data_cur );

    tuple = heap_form_tuple( tupdesc, user_ctx->values, user_ctx->nulls );

    FORMATTER_SET_TUPLE( fcinfo, tuple );
    FORMATTER_RETURN_TUPLE( tuple );
}

Datum
json_formatter_write( PG_FUNCTION_ARGS ) {
    HeapTupleHeader     rec = PG_GETARG_HEAPTUPLEHEADER(0);
    TupleDesc           tupdesc;
    HeapTupleData       tuple;
    MemoryContext       mc, omc;
    user_write_ctx_t    *user_ctx;
    char                *jbuf;
    int                 jbufn;
    int                 ncolumns = 0;
    int                 i = 0;

    /**
     * Must be called via the ext tab format manager
     */
    if( !CALLED_AS_FORMATTER(fcinfo) )
        elog( ERROR, "json_formatter_write: not called by format manager" );

    tupdesc = FORMATTER_GET_TUPDESC( fcinfo );
    ncolumns = tupdesc->natts;

    user_ctx = (user_write_ctx_t *)FORMATTER_GET_USER_CTX( fcinfo );

    /**
     * First call to formatter, setup context
     */
    if( user_ctx == NULL ) {
        user_ctx = palloc( sizeof(user_write_ctx_t) );

        user_ctx->ncolumns = ncolumns;
        user_ctx->j_root = json_object();
        user_ctx->j_vals = palloc( sizeof(json_t*) * ncolumns );
        user_ctx->buf = palloc( VARHDRSZ );
        user_ctx->dbvalues = palloc( sizeof(Datum) * ncolumns );
        user_ctx->dbnulls = palloc( sizeof(bool) * ncolumns );

        for( i=0; i < ncolumns; i++ ) {
            Oid         type = tupdesc->attrs[i]->atttypid;
            char        *dbcolname, *tofree;
            char        *jobjname, *pjobjname;
            int         ret=0;
            json_t      *j_parent = user_ctx->j_root;

            tofree = dbcolname = strdup( tupdesc->attrs[i]->attname.data );
            user_ctx->j_vals[i] = NULL;

            /**
             * Deal with column names containing the nested seperator
             */
            while( (jobjname = strsep( &dbcolname, "." )) != NULL ) {
                if( user_ctx->j_vals[i] )
                    j_parent = user_ctx->j_vals[i];

                //nested objects may already exist, add only if needed
                user_ctx->j_vals[i] = json_object_get( j_parent, jobjname );

                if( !user_ctx->j_vals[i] ) {
                    //generic objects will be replaced later if necessary
                    user_ctx->j_vals[i] = json_object();
                    ret = json_object_set( j_parent, jobjname, user_ctx->j_vals[i] );
                    if( ret < 0 ) {
                        elog( ERROR, "Failed to append nested JSON object" );
                    }
                }

                //keep a pointer to the current sub-object name
                pjobjname = jobjname;
            }

            switch( type ) {
                case INT2OID:
                case INT4OID:
                case INT8OID:
                {
                    user_ctx->j_vals[i] = json_integer(0);
                    if( user_ctx->j_vals[i] == NULL ) {
                        elog( ERROR, "Could not initialize json integer" );
                    }

                    break;
                }
                case FLOAT4OID:
                case FLOAT8OID:
                {
                    user_ctx->j_vals[i] = json_real(0.0);
                    if( user_ctx->j_vals[i] == NULL ) {
                        elog( ERROR, "Could not initialize json real" );
                    }

                    break;
                }
                case TEXTOID:
                case VARCHAROID:
                {
                    user_ctx->j_vals[i] = json_string( "" );
                    if( user_ctx->j_vals[i] == NULL ) {
                        elog( ERROR, "Could not initialize json string" );
                    }

                    break;
                }
                case BOOLOID:
                {
                  user_ctx->j_vals[i] = json_boolean(0);
                  if( user_ctx->j_vals[i] == NULL ) {
                      elog( ERROR, "Could not initialize json boolean" );
                  }

                  break;
                }
                default:
                {
                    elog( ERROR, "Type not supported" );
                }
            }

            ret = json_object_set( j_parent, pjobjname, user_ctx->j_vals[i] );
            if( ret < 0 ) {
                elog( ERROR, "Failed to append to JSON object" );
            }
        }

        FORMATTER_SET_USER_CTX( fcinfo, user_ctx );
    }

    /**
     * Clear column buffers
     */
    MemSet( user_ctx->dbvalues, 0, ncolumns * sizeof(Datum) );
    MemSet( user_ctx->dbnulls, false, ncolumns * sizeof(bool) );

    /**
     * Switch memory context
     */
    mc = FORMATTER_GET_PER_ROW_MEM_CTX( fcinfo );
    omc = MemoryContextSwitchTo( mc );

    /**
     * Break input record into fields
     */
    tuple.t_len = HeapTupleHeaderGetDatumLength( rec );
    ItemPointerSetInvalid( &(tuple.t_self) );
    tuple.t_data = rec;
    heap_deform_tuple( &tuple, tupdesc, user_ctx->dbvalues, user_ctx->dbnulls );

    /**
     * Iterate over columns from database
     */
    for( i=0; i < ncolumns; i++ ) {
        Oid     type = tupdesc->attrs[i]->atttypid;
        int     ret = 0;

        switch( type ) {
            case INT2OID:
            case INT4OID:
            case INT8OID:
            {
                int64 value;

                if( user_ctx->dbnulls[i] )
                    value = 0;
                else
                    value = DatumGetInt64( user_ctx->dbvalues[i] );

                ret = json_integer_set( user_ctx->j_vals[i], value );
                if( ret < 0 ) {
                    MemoryContextSwitchTo( omc );
                    elog( ERROR, "Unable to set int value for column '%s'", tupdesc->attrs[i]->attname.data );
                }
                break;
            }
            case FLOAT4OID:
            {
                float4 value;

                if( user_ctx->dbnulls[i] )
                    value = 0;
                else
                    value = DatumGetFloat4( user_ctx->dbvalues[i] );

                ret = json_real_set( user_ctx->j_vals[i], value );
                if( ret < 0 ) {
                    MemoryContextSwitchTo( omc );
                    elog( ERROR, "Unable to set float value for column '%s'", tupdesc->attrs[i]->attname.data );
                }
                break;
            }
            case FLOAT8OID:
            {
                float8 value;

                if( user_ctx->dbnulls[i] )
                    value = 0;
                else
                    value = DatumGetFloat8( user_ctx->dbvalues[i] );

                ret = json_real_set( user_ctx->j_vals[i], value );
                if( ret < 0 ) {
                    MemoryContextSwitchTo( omc );
                    elog( ERROR, "Unable to set float value for column '%s'", tupdesc->attrs[i]->attname.data );
                }
                break;
            }
            case TEXTOID:
            case VARCHAROID:
            {
                char *value;

                if( user_ctx->dbnulls[i] ) {
                    value = "";
                } else {
                    value = DatumGetCString( DirectFunctionCall1( textout, user_ctx->dbvalues[i] ) );
                }

                ret = json_string_set( user_ctx->j_vals[i], value );
                if( ret < 0 ) {
                    MemoryContextSwitchTo( omc );
                    elog( ERROR, "Unable to set string value for column '%s'", tupdesc->attrs[i]->attname.data );
                }

                break;
            }
            default:
            {
                MemoryContextSwitchTo( omc );
                elog( ERROR, "Type of column '%s' not supported", tupdesc->attrs[i]->attname.data );
            }
        }
    }

    /**
     * Dump out JSON object as a string
     */
    jbuf = json_dumps( user_ctx->j_root, 0 );
    jbufn = strlen( jbuf );

    /**
     * Copy the resulting string into our bytea buffer with a trailing newline
     */
    MemoryContextSwitchTo( omc );

    if( user_ctx->buf )
        pfree( user_ctx->buf );

    user_ctx->buf = palloc( jbufn + VARHDRSZ + 1 );
    char *data = VARDATA(user_ctx->buf);

    SET_VARSIZE( user_ctx->buf, jbufn + VARHDRSZ + 1 );
    memcpy( VARDATA(user_ctx->buf), jbuf, jbufn );
    memcpy( &data[jbufn], "\n", 1 );
    free( jbuf );

    PG_RETURN_BYTEA_P( user_ctx->buf );
}
