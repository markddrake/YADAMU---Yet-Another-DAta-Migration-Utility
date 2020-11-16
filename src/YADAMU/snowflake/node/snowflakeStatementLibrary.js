"use strict" 

const YadamuConstants = require('../../common/yadamuConstants.js');

class SnowflakeStatementLibrary {

  static get SQL_CONFIGURE_CONNECTION()                       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()                         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()                         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_BEGIN_TRANSACTION()                          { return _SQL_BEGIN_TRANSACTION }  
  static get SQL_COMMIT_TRANSACTION()                         { return _SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()                       { return _SQL_ROLLBACK_TRANSACTION }

  get SQL_SCHEMA_INFORMATION() {
     this._SQL_SCHEMA_INFORMATION = this._SQL_SCHEMA_INFORMATION || (() => { 
       return `select t.table_schema   "TABLE_SCHEMA"
                     ,t.table_name   "TABLE_NAME"
                     ,concat('[',listagg(concat('"',c.column_name,'"'),',') within group (order by ordinal_position),']') "COLUMN_NAME_ARRAY"
                     ,concat('[',listagg(concat('"',data_type,'"'),',') within group (order by ordinal_position),']') "DATA_TYPE_ARRAY"
                     ,concat('[',listagg(case
                                   when (numeric_precision is not null) and (numeric_scale is not null) 
                                     then concat('"',numeric_precision,',',numeric_scale,'"')
                                   when (numeric_precision is not null) 
                                     then concat('"',numeric_precision,'"')
                                   when (datetime_precision is not null)
                                     then concat('"',datetime_precision,'"')
                                   when (character_maximum_length is not null)
                                     then concat('"',character_maximum_length,'"')
                                   else
                                     '""'
                                 end
                                ,','                   
                               ) within group (order by ordinal_position)
                             ,']') "SIZE_CONSTRAINT_ARRAY"
                     ,listagg(case
                               when c.data_type = 'VARIANT' then
                                 concat('TO_VARCHAR("',column_name,'") "',column_name,'"')
                               when c.data_type = 'TIME' then
                                 concat('cast(concat(''1971-01-01T'',','"',column_name,'"',') as Timestamp)')
                               else
                                 concat('"',column_name,'"')
                               end
                              ,','
                             ) within group (order by ordinal_position) "CLIENT_SELECT_LIST"
                 from "${this.dbi.parameters.YADAMU_DATABASE}"."INFORMATION_SCHEMA"."COLUMNS" c, "${this.dbi.parameters.YADAMU_DATABASE}"."INFORMATION_SCHEMA"."TABLES" t
                where t.table_name = c.table_name
                  and t.table_schema = c.table_schema
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
module.exports = SnowflakeStatementLibrary

const _SQL_CONFIGURE_CONNECTION = `alter session set autocommit=false timezone='UTC' TIMESTAMP_OUTPUT_FORMAT='YYYY-MM-DDTHH24:MI:SS.FF9TZH:TZM' TIMESTAMP_NTZ_OUTPUT_FORMAT='YYYY-MM-DDTHH24:MI:SS.FF9TZH:TZM' TIME_INPUT_FORMAT='YYYY-MM-DDTHH24:MI:SS.FF9TZH:TZM' GEOGRAPHY_OUTPUT_FORMAT ='WKB'`

const _SQL_SYSTEM_INFORMATION   = `select CURRENT_WAREHOUSE() WAREHOUSE, CURRENT_DATABASE() DATABASE_NAME, CURRENT_SCHEMA() SCHEMA, CURRENT_ACCOUNT() ACCOUNT, CURRENT_VERSION() DATABASE_VERSION, CURRENT_CLIENT() CLIENT`
    


const _SQL_BEGIN_TRANSACTION    = `begin`;

const _SQL_COMMIT_TRANSACTION   = `commit`;

const _SQL_ROLLBACK_TRANSACTION = `rollback`;

