"use strict" 

const YadamuConstants = require('../../common/yadamuConstants.js');

class VerticaStatementLibrary {
    
  static get SQL_CONFIGURE_CONNECTION()       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()         { return _SQL_GET_DLL_STATEMENTS }
  // static get SQL_SCHEMA_INFORMATION()         { return _SQL_SCHEMA_INFORMATION } 
  static get SQL_BEGIN_TRANSACTION()          { return _SQL_BEGIN_TRANSACTION }
  static get SQL_COMMIT_TRANSACTION()         { return _SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()       { return _SQL_ROLLBACK_TRANSACTION }
  static get SQL_CREATE_SAVE_POINT()          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return _SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()         { return _SQL_RELEASE_SAVE_POINT }
  static get SQL_POSTGIS_INFO()               { return _SQL_POSTGIS_INFO }
  
  static SQL_SCHEMA_INFORMATION(schema) {
    return `select c.TABLE_SCHEMA
	              ,c.TABLE_NAME
				  ,c.COLUMN_NAME
				  ,case 
				     when tc.PREDICATE like 'YADAMU.IS_JSON(%)' then 
				       'json'
				     when tc.PREDICATE like 'YADAMU.IS_XML(%)' then 
					   'xml'
					 else
					   DATA_TYPE 
				   end "DATA_TYPE"
				 ,DATA_TYPE_LENGTH
				 ,CHARACTER_MAXIMUM_LENGTH
				 ,NUMERIC_PRECISION
				 ,NUMERIC_SCALE
				 ,DATETIME_PRECISION
				 ,INTERVAL_PRECISION
             from V_CATALOG.COLUMNS c
             left join V_CATALOG.TABLES t
               on t.table_name = c.table_name 
              and t.table_schema = c.table_schema
             left outer join V_CATALOG.TABLE_CONSTRAINTS tc
               on t.TABLE_ID = tc.TABLE_ID 
              and tc.CONSTRAINT_TYPE = 'c'
		      and (
				    tc.PREDICATE = 'YADAMU.IS_JSON(' || t.TABLE_NAME || '.' || c.COLUMN_NAME || ')'
				 or
				    tc.PREDICATE = 'YADAMU.IS_XML(' || t.TABLE_NAME || '.' || c.COLUMN_NAME || ')'
			      )     
            where t.table_schema = '${schema}'
            order by t.TABLE_NAME,
		             c.ORDINAL_POSITION`
  }
}

module.exports = VerticaStatementLibrary

const _SQL_CONFIGURE_CONNECTION = `SET SESSION AUTOCOMMIT TO OFF`


const _SQL_SCHEMA_INFORMATION   = 
`select c.TABLE_SCHEMA, c.TABLE_NAME, DATA_TYPE, DATA_TYPE_LENGTH, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, INTERVAL_PRECISION
                from V_CATALOG.COLUMNS c
                     left join V_CATALOG.TABLES t
                        on t.table_name = c.table_name 
                       and t.table_schema = c.table_schema
               where t.table_schema = $1
               order by t.TABLE_NAME,
			            c.ORDINAL_POSITION`;
 
const _SQL_SYSTEM_INFORMATION   = `select current_database() database_name, current_user, session_user, version(), session_id from sessions`;

const _SQL_BEGIN_TRANSACTION    = `begin transaction`

const _SQL_COMMIT_TRANSACTION   = `commit transaction`

const _SQL_ROLLBACK_TRANSACTION = `rollback transaction`

const _SQL_CREATE_SAVE_POINT    = `savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const _SQL_RESTORE_SAVE_POINT   = `rollback to savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const _SQL_RELEASE_SAVE_POINT   = `release savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const _SQL_POSTGIS_INFO         = `select  PostGIS_version() "POSTGIS"`

const _PGOID_DATE         = 1082; 
const _PGOID_TIMESTAMP    = 1114;
const _PGOID_TIMESTAMP_TZ = 1118;

