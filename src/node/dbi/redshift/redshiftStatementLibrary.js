
import YadamuConstants from '../../lib/yadamuConstants.js';

class RedshiftStatementLibrary {
    
  static get SQL_CONFIGURE_CONNECTION()       { return SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()         { return SQL_SYSTEM_INFORMATION }
  static get SQL_SCHEMA_INFORMATION()         { return SQL_SCHEMA_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()         { return SQL_GET_DLL_STATEMENTS }
  static get SQL_BEGIN_TRANSACTION()          { return SQL_BEGIN_TRANSACTION }
  static get SQL_COMMIT_TRANSACTION()         { return SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()       { return SQL_ROLLBACK_TRANSACTION }
  static get SQL_CREATE_SAVE_POINT()          { return SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()         { return SQL_RELEASE_SAVE_POINT }
  
  static get SQL_COPY_STATUS()                { return SQL_COPY_STATUS }
  static get SQL_COPY_ERRORS()                { return SQL_COPY_ERRORS }
  static get SQL_COPY_ERROR_SUMMARY()         { return SQL_COPY_ERROR_SUMMARY }
  static get SQL_SUPER_ERROR_SUMMARY()        { return SQL_SUPER_ERROR_SUMMARY }
}

export { RedshiftStatementLibrary as default }

const SQL_CONFIGURE_CONNECTION = `SET timezone to 'UTC'; SET extra_float_digits to 2; SET enable_case_sensitive_identifier TO true;`

const SQL_SCHEMA_INFORMATION  =
`           select t.table_schema as "TABLE_SCHEMA"
                 ,t.table_name as "TABLE_NAME"
	             ,c.column_name as "COLUMN_NAME"
	             ,c.data_type as "DATA_TYPE"
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
                  as "SIZE_CONSTRAINT"
            from SVV_COLUMNS c
  			left join SVV_TABLES t
			  on ((t.table_schema, t.table_name) = (c.table_schema, c.table_name))
           where t.table_type = 'BASE TABLE'
             and t.table_schema =  $1
            order by t.table_schema, t.table_name, c.ORDINAL_POSITION`
			
const SQL_SYSTEM_INFORMATION   = `select current_database() as database_name,current_user, session_user, version() as database_version, right(to_char(current_timestamp,'YYYY-MM-DD"T"HH24:MI:SSOF')::char(39),6) as timezone, YADAMU_INSTANCE_ID() as YADAMU_INSTANCE_ID, YADAMU_INSTALLATION_TIMESTAMP() as YADAMU_INSTALLATION_TIMESTAMP`;

const SQL_BEGIN_TRANSACTION    = `begin transaction`

const SQL_COMMIT_TRANSACTION   = `commit transaction`

const SQL_ROLLBACK_TRANSACTION = `rollback transaction`

const SQL_CREATE_SAVE_POINT    = `savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const SQL_RESTORE_SAVE_POINT   = `rollback to savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const SQL_RELEASE_SAVE_POINT   = `release savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const SQL_COPY_STATUS          = `select lines_scanned from stl_load_commits where query = pg_last_copy_id()`

const SQL_COPY_ERRORS          = `select count(*) from stl_load_errors where query = pg_last_copy_id()`

const SQL_COPY_ERROR_SUMMARY   = `select line_number, colname, err_reason, err_code, col_length, query, filename, type, position, raw_field_value FROM stl_load_errors e where query = pg_last_copy_id()` 

const SQL_SUPER_ERROR_SUMMARY  = `select line_number, colname, err_reason, err_code, col_length, query, filename, type, position, raw_field_value FROM stl_load_errors e where starttime = (select max(starttime) from stl_load_errors where session = pg_backend_pid()) and session = pg_backend_pid()` 
