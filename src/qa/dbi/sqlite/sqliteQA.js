import SqliteDBI        from '../../../node/dbi/sqlite/sqliteDBI.js';
import SqliteConstants  from '../../../node/dbi/sqlite/sqliteConstants.js';
import SqliteStatementLibrary from '../../../node/dbi/sqlite/sqliteStatementLibrary.js';

import Yadamu           from '../../core/yadamu.js';
import YadamuQALibrary  from '../../lib/yadamuQALibrary.js';

class SqliteQA extends YadamuQALibrary.qaMixin(SqliteDBI) {

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
    this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({},Yadamu.DBI_PARAMETERS,SqliteConstants.DBI_PARAMETERS,Yadamu.QA_CONFIGURATION[SqliteConstants.DATABASE_KEY] || {},{RDBMS: SqliteConstants.DATABASE_KEY}))
    return this.#DBI_PARAMETERS
  }

  get DBI_PARAMETERS() {
    return SqliteQA.DBI_PARAMETERS
  }

  constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)
  }

  /*
  **
  ** SQLite has no schema heirarchy: the database file operates as a single namespace. "Recreate Schema"
  ** therefore means dropping all user tables in the target database file. Foreign key enforcement is
  ** disabled while dropping to avoid implicit DELETE failures on child tables.
  **
  */
  
  async recreateSchema() {
    const userTables = await this.executeSQL(SqliteStatementLibrary.SQL_LIST_USER_TABLES(this.SCHEMA_NAME))
    await this.executeSQL('PRAGMA foreign_keys = OFF')
    for (const table of userTables) {
      const tableName = table.TABLE_NAME.replace(/"/g,'""')
      await this.executeSQL(`DROP TABLE IF EXISTS ${this.qualifiedTableName(tableName)}`)
    }
    await this.executeSQL('PRAGMA foreign_keys = ON')
  }

}

export { SqliteQA as default }
