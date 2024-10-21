
import YadamuConstants from '../../lib/yadamuConstants.js';

class PostgresStatementLibrary {
    
  static get SQL_CONFIGURE_CONNECTION()       { return SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()         { return SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()         { return SQL_GET_DLL_STATEMENTS }
  static get SQL_SCHEMA_INFORMATION()         { return SQL_SCHEMA_INFORMATION } 
  static get SQL_BEGIN_TRANSACTION()          { return SQL_BEGIN_TRANSACTION }
  static get SQL_COMMIT_TRANSACTION()         { return SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()       { return SQL_ROLLBACK_TRANSACTION }
  static get SQL_CREATE_SAVE_POINT()          { return SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()         { return SQL_RELEASE_SAVE_POINT }
  static get SQL_POSTGIS_INFO()               { return SQL_POSTGIS_INFO }

}

export { PostgresStatementLibrary as default }

const SQL_CONFIGURE_CONNECTION = `set timezone to 'UTC'; SET extra_float_digits to 3; SET Intervalstyle = 'iso_8601'`

const SQL_SCHEMA_INFORMATION   = `select * from YADAMU.YADAMU_EXPORT($1,$2,$3)`;
 
const SQL_SYSTEM_INFORMATION   = `select current_database() database_name,current_user, session_user, current_setting('server_version_num') database_version, right(to_char(current_timestamp,'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM'),6) timezone, YADAMU.YADAMU_INSTANCE_ID() YADAMU_INSTANCE_ID, YADAMU.YADAMU_INSTALLATION_TIMESTAMP() YADAMU_INSTALLATION_TIMESTAMP`;

const SQL_BEGIN_TRANSACTION    = `begin transaction`

const SQL_COMMIT_TRANSACTION   = `commit transaction`

const SQL_ROLLBACK_TRANSACTION = `rollback transaction`

const SQL_CREATE_SAVE_POINT    = `savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const SQL_RESTORE_SAVE_POINT   = `rollback to savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const SQL_RELEASE_SAVE_POINT   = `release savepoint "${YadamuConstants.SAVE_POINT_NAME}"`;

const SQL_POSTGIS_INFO         = `select  PostGIS_version() "POSTGIS"`
