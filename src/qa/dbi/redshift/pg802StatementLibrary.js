"use strict" 

import _RedshiftStatementLibrary     from '../../../node/dbi/redshift/redshiftStatementLibrary.js'

class RedshiftStatementLibrary extends _RedshiftStatementLibrary  {
    
  static get SQL_CONFIGURE_CONNECTION()       { return _SQL_CONFIGURE_CONNECTION }
  static get SQL_SYSTEM_INFORMATION()         { return _SQL_SYSTEM_INFORMATION }
  static get SQL_SCHEMA_INFORMATION()         { return super.SQL_SCHEMA_INFORMATION }
  static get SQL_GET_DLL_STATEMENTS()         { return super.SQL_GET_DLL_STATEMENTS }
  static get SQL_BEGIN_TRANSACTION()          { return super.SQL_BEGIN_TRANSACTION }
  static get SQL_COMMIT_TRANSACTION()         { return super.SQL_COMMIT_TRANSACTION }
  static get SQL_ROLLBACK_TRANSACTION()       { return super.SQL_ROLLBACK_TRANSACTION }
  static get SQL_CREATE_SAVE_POINT()          { return super.SQL_CREATE_SAVE_POINT }  
  static get SQL_RESTORE_SAVE_POINT()         { return super.SQL_RESTORE_SAVE_POINT }
  static get SQL_RELEASE_SAVE_POINT()         { return super.SQL_RELEASE_SAVE_POINT }
  static get SQL_COPY_STATUS()                { return super.SQL_COPY_STATUS }
  static get SQL_COPY_ERRORS()                { return super.SQL_COPY_ERRORS }
  static get SQL_COPY_ERROR_SUMMARY()         { return super.SQL_COPY_ERROR_SUMMARY }
  static get SQL_SUPER_ERROR_SUMMARY()        { return super.SQL_SUPER_ERROR_SUMMARY }
}

export { RedshiftStatementLibrary as default }

const _SQL_CONFIGURE_CONNECTION = `SET timezone to 'UTC'; SET extra_float_digits to 2;`

const _SQL_SYSTEM_INFORMATION   = `select current_database() as database_name,current_user, session_user, version() as database_version, '+00:00' as timezone, YADAMU_INSTANCE_ID() as YADAMU_INSTANCE_ID, YADAMU_INSTALLATION_TIMESTAMP() as YADAMU_INSTALLATION_TIMESTAMP`;

