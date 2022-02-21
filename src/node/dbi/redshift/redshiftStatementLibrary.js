"use strict" 

import YadamuConstants from '../../lib/yadamuConstants.js';

class RedshiftStatementLibrary {
    
  static get SQL_CONFIGURE_CONNECTION()       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_SCHEMA_INFORMATION()         { return _SQL_SCHEMA_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_BEGIN_TRANSACTION()          { return _SQL_BEGIN_TRANSACTION }
  static get SQL_COMMIT_TRANSACTION()         { return _SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()       { return _SQL_ROLLBACK_TRANSACTION }
  static get SQL_CREATE_SAVE_POINT()          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return _SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()         { return _SQL_RELEASE_SAVE_POINT }
  
  static get SQL_COPY_STATUS()                { return _SQL_COPY_STATUS }
  static get SQL_COPY_ERRORS()                { return _SQL_COPY_ERRORS }
  static get SQL_COPY_ERROR_SUMMARY()         { return _SQL_COPY_ERROR_SUMMARY }
  static get SQL_SUPER_ERROR_SUMMARY()        { return _SQL_SUPER_ERROR_SUMMARY }
}

export { RedshiftStatementLibrary as default }

const _SQL_CONFIGURE_CONNECTION = `set timezone to 'UTC'; SET extra_float_digits to 2; SET enable_case_sensitive_identifier TO true;`

 const _SQL_SCHEMA_INFORMATION  =
`           select t.table_schema "TABLE_SCHEMA"
                 ,t.table_name "TABLE_NAME"
	             ,c.column_name "COLUMN_NAME"
	             ,c.data_type "DATA_TYPE"
                 ,case
                              when (c.numeric_precision is not null) and (c.numeric_scale is not null) then
                                cast(c.numeric_precision as varchar) || ',' || cast(c.numeric_scale as varchar)
                              when (c.numeric_precision is not null) then 
                                cast(c.numeric_precision as varchar)
                              when (c.character_maximum_length is not null) then 
                                cast(c.character_maximum_length as varchar)
                              when (c.datetime_precision is not null) then 
                                cast(c.datetime_precision as varchar)
                            end
                  "SIZE_CONSTRAINT"
            from SVV_COLUMNS c
  			left join SVV_TABLES t
			  on ((t.table_schema, t.table_name) = (c.table_schema, c.table_name))
           where t.table_type = 'BASE TABLE'
             and t.table_schema =  $1
            order by t.table_schema, t.table_name, c.ORDINAL_POSITION`
			
const _SQL_SYSTEM_INFORMATION   = `select current_database() database_name,current_user, session_user, version() database_version, right(to_char(current_timestamp,'YYYY-MM-DD"T"HH24:MI:SSOF'),6) timezone, YADAMU_INSTANCE_ID() YADAMU_INSTANCE_ID, YADAMU_INSTALLATION_TIMESTAMP() YADAMU_INSTALLATION_TIMESTAMP`;

const _SQL_BEGIN_TRANSACTION    = `begin transaction`

const _SQL_COMMIT_TRANSACTION   = `commit transaction`

const _SQL_ROLLBACK_TRANSACTION = `rollback transaction`

const _SQL_CREATE_SAVE_POINT    = `savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const _SQL_RESTORE_SAVE_POINT   = `rollback to savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const _SQL_RELEASE_SAVE_POINT   = `release savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const _SQL_COPY_STATUS          = `select lines_scanned from stl_load_commits where query = pg_last_copy_id()`

const _SQL_COPY_ERRORS          = `select count(*) from stl_load_errors where query = pg_last_copy_id()`

const _SQL_COPY_ERROR_SUMMARY   = `select line_number, colname, err_reason, err_code, col_length, query, filename, type, position, raw_field_value FROM stl_load_errors e where query = pg_last_copy_id()` 

const _SQL_SUPER_ERROR_SUMMARY  = `select line_number, colname, err_reason, err_code, col_length, query, filename, type, position, raw_field_value FROM stl_load_errors e where starttime = (select max(starttime) from stl_load_errors where session = pg_backend_pid()) and session = pg_backend_pid()` 
