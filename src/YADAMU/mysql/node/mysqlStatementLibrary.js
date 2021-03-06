"use strict" 

const YadamuConstants = require('../../common/yadamuConstants.js');

class MySQLStatementLibrary {
    
  // Until we have static constants

  static get SQL_CONFIGURE_CONNECTION()                       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_GET_CONNECTION_INFORMATION()                 { return _SQL_GET_CONNECTION_INFORMATION }
  static get SQL_SHOW_SYSTEM_VARIABLES()                      { return _SQL_SHOW_SYSTEM_VARIABLES }
  static get SQL_SYSTEM_INFORMATION()                         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()                         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_INFORMATION_SCHEMA_FROM_CLAUSE()             { return _SQL_INFORMATION_SCHEMA_FROM_CLAUSE }
  static get SQL_CREATE_SAVE_POINT()                          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()                         { return _SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()                         { return _SQL_RELEASE_SAVE_POINT }

  get SCHEMA_INFORMATION_SELECT_CLAUSE() {
    this._SCHEMA_INFORMATION_SELECT_CLAUSE = this._SCHEMA_INFORMATION_SELECT_CLAUSE || (() => {       
      return `select c.table_schema "TABLE_SCHEMA"
                    ,c.table_name "TABLE_NAME"
                    ,concat('[',group_concat(concat('"',column_name,'"') order by ordinal_position separator ','),']')  "COLUMN_NAME_ARRAY"
                    ,concat('[',group_concat(case 
                                               when column_type = 'tinyint(1)' then 
                                                 json_quote('${this.dbi.TREAT_TINYINT1_AS_BOOLEAN ? 'boolean' : 'tinyint(1)'}')
                                               else 
                                                 json_quote(data_type)
                                             end 
                                             order by ordinal_position separator ','),']')  "DATA_TYPE_ARRAY"
                    ,concat('[',group_concat(json_quote(case 
                                                          when column_type = 'tinyint(1)' then
                                                            ${this.dbi.TREAT_TINYINT1_AS_BOOLEAN ? "''" : "'3'"}
                                                          when (numeric_precision is not null) and (numeric_scale is not null) then
                                                            concat(numeric_precision,',',numeric_scale) 
                                                          when (numeric_precision is not null) then
                                                            case
                                                              when data_type = 'bit' then
                                                                numeric_precision                                  
                                                              when column_type like '%unsigned' then 
                                                                numeric_precision
                                                              else
                                                                numeric_precision + 1
                                                            end
                                                          when (datetime_precision is not null) then
                                                            datetime_precision
                                                          when (character_maximum_length is not null) then
                                                            character_maximum_length
                                                          else   
                                                            ''   
                                                        end
                                                       ) 
                                             order by ordinal_position separator ','
                                            ),']') "SIZE_CONSTRAINT_ARRAY"
                    ,group_concat(
                          case 
                            when data_type in ('date','time','datetime','timestamp') then
                              -- Force ISO 8601 rendering of value 
                              concat('DATE_FORMAT(convert_tz("', column_name, '", @@session.time_zone, ''+00:00''),''%Y-%m-%dT%T.%fZ'')',' "',column_name,'"')
                            when data_type = 'bit' then 
                              concat('conv("', column_name, '",10,2) "',column_name,'"')
                            when data_type = 'year' then
                              -- Prevent rendering of value as base64:type13: 
                              concat('CAST("', column_name, '"as DECIMAL) "',column_name,'"')
                            when data_type in ('point','linestring','polygon','geometry','multipoint','multilinestring','multipolygon','geomcollection') then
                              -- Force ${this.spatialFormat} rendering of value
                              concat('${this.dbi.SPATIAL_SERIALIZER}"', column_name, '") "',column_name,'"')
                            when data_type = 'float' then
                              -- Render Floats with greatest possible precision 
                             concat('cast("',column_name,'" as DOUBLE) "',column_name,'"')
                            else
                              concat('"',column_name,'"')
                          end
                          order by ordinal_position separator ','
                     ) "CLIENT_SELECT_LIST"`;
    })();
    return this._SCHEMA_INFORMATION_SELECT_CLAUSE
  }   
  
  get SQL_INFORMATION_SCHEMA_FROM_CLAUSE() { return MySQLStatementLibrary.SQL_INFORMATION_SCHEMA_FROM_CLAUSE }
  
  get SQL_SCHEMA_INFORMATION() {
    this._SQL_SCHEMA_INFORMATION = this._SQL_SCHEMA_INFORMATION || (() => {       
      return `${this.SCHEMA_INFORMATION_SELECT_CLAUSE}${this.SQL_INFORMATION_SCHEMA_FROM_CLAUSE}` 
    })();
    return this._SQL_SCHEMA_INFORMATION
  }   

  constructor(dbi) {
    this.dbi = dbi
  }
}  

module.exports = MySQLStatementLibrary

const _SQL_CONFIGURE_CONNECTION       = `SET AUTOCOMMIT = 0, TIME_ZONE = '+00:00',SESSION INTERACTIVE_TIMEOUT = 600000, WAIT_TIMEOUT = 600000, SQL_MODE='ANSI_QUOTES,PAD_CHAR_TO_FULL_LENGTH', GROUP_CONCAT_MAX_LEN = 1024000, GLOBAL LOCAL_INFILE = 'ON'`

const _SQL_GET_CONNECTION_INFORMATION = `select version() "DATABASE_VERSION"`

const _SQL_SHOW_SYSTEM_VARIABLES      = `show variables where Variable_name='lower_case_table_names'`;

const _SQL_SYSTEM_INFORMATION         = `select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID", @@session.time_zone "SESSION_TIME_ZONE", @@character_set_server "SERVER_CHARACTER_SET", @@character_set_database "DATABASE_CHARACTER_SET"`;                     

const _SQL_INFORMATION_SCHEMA_FROM_CLAUSE =
`   from information_schema.columns c, information_schema.tables t
  where t.table_name = c.table_name 
    and c.extra <> 'VIRTUAL GENERATED'
    and t.table_schema = c.table_schema
    and t.table_type = 'BASE TABLE'
    and t.table_schema = ?
      group by t.table_schema, t.table_name`;
     
const _SQL_CREATE_SAVE_POINT  = `SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RESTORE_SAVE_POINT = `ROLLBACK TO SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RELEASE_SAVE_POINT = `RELEASE SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;
