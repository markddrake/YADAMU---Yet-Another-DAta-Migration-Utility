
import YadamuConstants from '../../lib/yadamuConstants.js';

class VerticaStatementLibrary {
    
  static get SQL_CONFIGURE_CONNECTION()       { return SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()         { return SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()         { return SQL_GET_DLL_STATEMENTS }
  // static get SQL_SCHEMA_INFORMATION()         { return SQL_SCHEMA_INFORMATION } 
  static get SQL_BEGIN_TRANSACTION()          { return SQL_BEGIN_TRANSACTION }
  static get SQL_COMMIT_TRANSACTION()         { return SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()       { return SQL_ROLLBACK_TRANSACTION }
  static get SQL_CREATE_SAVE_POINT()          { return SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()         { return SQL_RELEASE_SAVE_POINT }
  static get SQL_POSTGIS_INFO()               { return SQL_POSTGIS_INFO }
  
  static SQL_SCHEMA_INFORMATION(schema) {
    return `select c.TABLE_SCHEMA
	              ,c.TABLE_NAME
				  ,c.COLUMN_NAME
				  ,case 
				     when tc.PREDICATE like 'YADAMU.IS_JSON(%)' then 
				       'JSON'
				     when tc.PREDICATE like 'YADAMU.IS_XML(%)' then 
					   'XML'
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

export { VerticaStatementLibrary as default }

const SQL_CONFIGURE_CONNECTION = `SET SESSION AUTOCOMMIT TO OFF; SET DATESTYLE TO ISO; SET INTERVALSTYLE TO PLAIN`;


const SQL_SCHEMA_INFORMATION   = 
`select c.TABLE_SCHEMA, c.TABLE_NAME, DATA_TYPE, DATA_TYPE_LENGTH, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, DATETIME_PRECISION, INTERVAL_PRECISION
                from V_CATALOG.COLUMNS c
                     left join V_CATALOG.TABLES t
                        on t.table_name = c.table_name 
                       and t.table_schema = c.table_schema
               where t.table_schema = $1
               order by t.TABLE_NAME,
			            c.ORDINAL_POSITION`;
 
const SQL_SYSTEM_INFORMATION   = `select current_database() database_name, current_user, session_user, version(), session_id, YADAMU.YADAMU_INSTANCE_ID(), YADAMU.YADAMU_INSTALLATION_TIMESTAMP() from sessions`;

const SQL_BEGIN_TRANSACTION    = `begin transaction`

const SQL_COMMIT_TRANSACTION   = `commit transaction`

const SQL_ROLLBACK_TRANSACTION = `rollback transaction`

/*
const _SQL_CREATE_SAVE_POINT    = `savepoint ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RESTORE_SAVE_POINT   = `rollback to savepoint ${YadamuConstants.SAVE_POINT_NAME}`;

const _SQL_RELEASE_SAVE_POINT   = `release savepoint ${YadamuConstants.SAVE_POINT_NAME}`;
*/

const SQL_CREATE_SAVE_POINT    = `savepoint my_savepoint`;

const SQL_RESTORE_SAVE_POINT   = `rollback to savepoint my_savepoint`;

const SQL_RELEASE_SAVE_POINT   = `release savepoint my_savepoint`;

