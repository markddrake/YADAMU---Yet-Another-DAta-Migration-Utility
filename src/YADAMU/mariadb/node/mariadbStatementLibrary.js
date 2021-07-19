"use strict" 

const YadamuConstants = require('../../common/yadamuConstants.js');

class MariadbStatementLibrary {
    
  // Until we have static constants

  static get SQL_CONFIGURE_CONNECTION()                       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_GET_CONNECTION_INFORMATION()                 { return _SQL_GET_CONNECTION_INFORMATION }
  static get SQL_SYSTEM_INFORMATION()                         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()                         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_CREATE_SAVE_POINT()                          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()                         { return _SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()                         { return _SQL_RELEASE_SAVE_POINT }
  
  get SQL_SCHEMA_INFORMATION() {
     this._SQL_SCHEMA_INFORMATION = this._SQL_SCHEMA_INFORMATION || (() => { 
       return `select c.table_schema "TABLE_SCHEMA"   
              ,c.table_name "TABLE_NAME"
              ,concat('[',group_concat(concat('"',column_name,'"') order by ordinal_position separator ','),']')  "COLUMN_NAME_ARRAY"
              ,concat(
                '[',
                 group_concat(
                   json_quote(case 
                                when cc.check_clause is not null then 
                                  'json'
                                when c.column_type = 'tinyint(1)' then 
                                  '${this.dbi.TREAT_TINYINT1_AS_BOOLEAN ? 'boolean' : 'tinyint(1)'}'
                                else 
                                  data_type
                              end
                             ) 
                    order by ordinal_position separator ','
                 ),
                 ']'
               ) "DATA_TYPE_ARRAY"
              ,concat(
                '[',
                group_concat(
                  json_quote(case 
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
                ),
                ']'
               ) "SIZE_CONSTRAINT_ARRAY"
              ,group_concat(
                    case 
                      when data_type in ('time') then
                         -- Force ISO 8601 rendering of value 
                         concat('DATE_FORMAT(convert_tz(addtime(''1970-01-01 00:00:00'',"', column_name, '"), @@session.time_zone, ''+00:00''),''%Y-%m-%dT%T.%fZ'')',' "',column_name,'"')
                      when data_type in ('date','datetime','timestamp') then
                        -- Force ISO 8601 rendering of value 
                        concat('DATE_FORMAT(convert_tz("', column_name, '", @@session.time_zone, ''+00:00''),''%Y-%m-%dT%T.%fZ'')',' "',column_name,'"')
  					  when data_type = 'bit' then 
						concat('BIN("', column_name, '") "',column_name,'"')
                      when data_type = 'year' then
                        -- Prevent rendering of value as base64:type13: 
                        concat('CAST("', column_name, '"as DECIMAL) "',column_name,'"')
                      when data_type in ('geometry','point','linestring','polygon','multipoint','multilinestring','multipolygon','geometrycollection','geomcollection') then
                        -- Force ${this.spatialFormat} rendering of value
                        concat('${this.dbi.SPATIAL_SERIALIZER}"', column_name, '") "',column_name,'"')
                      when data_type in ('float') then
                        -- Render Floats with greatest possible precision 
                        -- Render Floats and Double as String 
                        concat('cast(cast("',column_name,'" as DOUBLE) as CHAR)"',column_name,'"')						
                      when data_type in ('double','bigint') then
                        -- Render Floats and Double as String 
                        concat('CAST("', column_name, '" as CHAR) "',column_name,'"')
                      -- when data_type in ('decimal') then
                        -- Render Decimals without trailing '0's
                        -- concat('CONVERT(0 + trim("',column_name,'"),CHAR) "',column_name,'"')
                      else
                        concat('"',column_name,'"')
                    end
                    order by ordinal_position separator ','
               ) "CLIENT_SELECT_LIST"
                from information_schema.columns c
                     left join information_schema.tables t
                        on t.table_name = c.table_name 
                       and t.table_schema = c.table_schema
                     left outer join information_schema.check_constraints cc
                        on cc.table_name = c.table_name 
                       and cc.constraint_schema = c.table_schema
                       and check_clause = concat('json_valid("',column_name,'")')  
               where c.extra <> 'VIRTUAL GENERATED'
                 and t.table_type = 'BASE TABLE'
                 and t.table_schema = ?
            group by t.table_schema, t.table_name`;
    })();
    return this._SQL_SCHEMA_INFORMATION
  }     

  constructor(dbi) {
    this.dbi = dbi
  }
}

module.exports = MariadbStatementLibrary

const _SQL_CONFIGURE_CONNECTION       = `SET AUTOCOMMIT = 0, TIME_ZONE = '+00:00',SESSION INTERACTIVE_TIMEOUT = 600000, WAIT_TIMEOUT = 600000, SQL_MODE='ANSI_QUOTES,PAD_CHAR_TO_FULL_LENGTH', GROUP_CONCAT_MAX_LEN = 1024000, GLOBAL LOCAL_INFILE = 'ON'`

const _SQL_GET_CONNECTION_INFORMATION = `select substring(version(),1,instr(version(),'-Maria')-1) "DATABASE_VERSION"`

const _SQL_SYSTEM_INFORMATION         = `select database() "DATABASE_NAME", current_user() "CURRENT_USER", session_user() "SESSION_USER", version() "DATABASE_VERSION", @@version_comment "SERVER_VENDOR_ID", @@session.time_zone "SESSION_TIME_ZONE", @@character_set_server "SERVER_CHARACTER_SET", @@character_set_database "DATABASE_CHARACTER_SET", YADAMU_INSTANCE_ID() "YADAMU_INSTANCE_ID", YADAMU_INSTALLATION_TIMESTAMP() "YADAMU_INSTALLATION_TIMESTAMP"`;                     
 
const _SQL_CREATE_SAVE_POINT          = `SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RESTORE_SAVE_POINT         = `ROLLBACK TO SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RELEASE_SAVE_POINT         = `RELEASE SAVEPOINT ${YadamuConstants.SAVE_POINT_NAME}`;