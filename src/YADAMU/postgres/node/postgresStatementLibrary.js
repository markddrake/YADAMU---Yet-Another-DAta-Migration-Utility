"use strict" 

const YadamuConstants = require('../../common/yadamuConstants.js');

class PostgresStatementLibrary {
    
  static get SQL_CONFIGURE_CONNECTION()       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()         { return _SQL_GET_DLL_STATEMENTS }
  static get SQL_SCHEMA_INFORMATION()         { return _SQL_SCHEMA_INFORMATION } 
  static get SQL_BEGIN_TRANSACTION()          { return _SQL_BEGIN_TRANSACTION }
  static get SQL_COMMIT_TRANSACTION()         { return _SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()       { return _SQL_ROLLBACK_TRANSACTION }
  static get SQL_CREATE_SAVE_POINT()          { return _SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return _SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()         { return _SQL_RELEASE_SAVE_POINT }
  static get SQL_POSTGIS_INFO()               { return _SQL_POSTGIS_INFO }

}

module.exports = PostgresStatementLibrary

const _SQL_CONFIGURE_CONNECTION = `set timezone to 'UTC'; SET extra_float_digits to 3; SET Intervalstyle = 'iso_8601'`

const _SQL_SCHEMA_INFORMATION   = `select * from YADAMU_EXPORT($1,$2,$3)`;
 
const _SQL_SYSTEM_INFORMATION   = `select current_database() database_name,current_user, session_user, current_setting('server_version_num') database_version, right(to_char(current_timestamp,'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM'),6) timezone, YADAMU_INSTANCE_ID() YADAMU_INSTANCE_ID, YADAMU_INSTALLATION_TIMESTAMP() YADAMU_INSTALLATION_TIMESTAMP`;

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

