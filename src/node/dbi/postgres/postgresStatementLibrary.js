
import YadamuConstants from '../../lib/yadamuConstants.js';

class PostgresStatementLibrary {

  static #SQL_CONFIGURE_CONNECTION = `set timezone to 'UTC'; SET extra_float_digits to 3; SET Intervalstyle = 'iso_8601'`

  static #SQL_SCHEMA_INFORMATION   = `select * from YADAMU.YADAMU_EXPORT($1,$2,$3)`;
 
  static #SQL_SYSTEM_INFORMATION   = `select current_database() database_name,current_user, session_user, current_setting('server_version_num') database_version, right(to_char(current_timestamp,'YYYY-MM-DD"T"HH24:MI:SSTZH:TZM'),6) timezone, YADAMU.YADAMU_INSTANCE_ID() YADAMU_INSTANCE_ID, YADAMU.YADAMU_INSTALLATION_TIMESTAMP() YADAMU_INSTALLATION_TIMESTAMP, version() extended_version_information`;

  static #SQL_BEGIN_TRANSACTION    = `begin transaction`

  static #SQL_COMMIT_TRANSACTION   = `commit transaction`

  static #SQL_ROLLBACK_TRANSACTION = `rollback transaction`

  static #SQL_CREATE_SAVE_POINT    = `savepoint "${YadamuConstants.SAVE_POINT_NAME}"`

  static #SQL_RESTORE_SAVE_POINT   = `rollback to savepoint "${YadamuConstants.SAVE_POINT_NAME}"`

  static #SQL_RELEASE_SAVE_POINT   = `release savepoint "${YadamuConstants.SAVE_POINT_NAME}"`

  static #SQL_POSTGIS_INFO         = `select  PostGIS_version() "POSTGIS"`

  static #SQL_GET_IDENTITY_COLUMNS = `select concat('setval(pg_get_serial_sequence(''', table_name,''',''', column_name,'''), coalesce(MAX(',column_name,'), 1)) from ', table_name) from information_schema.columns where table_schema = $1 and is_identity = 'YES'`
    
  static get SQL_CONFIGURE_CONNECTION()       { return PostgresStatementLibrary.#SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()         { return PostgresStatementLibrary.#SQL_SYSTEM_INFORMATION }
  static get SQL_SCHEMA_INFORMATION()         { return PostgresStatementLibrary.#SQL_SCHEMA_INFORMATION } 
  static get SQL_BEGIN_TRANSACTION()          { return PostgresStatementLibrary.#SQL_BEGIN_TRANSACTION }
  static get SQL_COMMIT_TRANSACTION()         { return PostgresStatementLibrary.#SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()       { return PostgresStatementLibrary.#SQL_ROLLBACK_TRANSACTION }
  static get SQL_CREATE_SAVE_POINT()          { return PostgresStatementLibrary.#SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return PostgresStatementLibrary.#SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()         { return PostgresStatementLibrary.#SQL_RELEASE_SAVE_POINT }
  static get SQL_POSTGIS_INFO()               { return PostgresStatementLibrary.#SQL_POSTGIS_INFO }
  static get SQL_GET_IDENTITY_COLUMNS()       { return PostgresStatementLibrary.#SQL_GET_IDENTITY_COLUMNS }
  
}

export { PostgresStatementLibrary as default }

