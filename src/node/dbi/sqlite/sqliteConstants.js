import YadamuConstants from '../../lib/yadamuConstants.js';

class SqliteConstants {

  static get DATABASE_KEY()               { return 'sqlite' };
  static get DATABASE_VENDOR()            { return 'SQLite' };
  static get SOFTWARE_VENDOR()            { return 'SQLite Consortium' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "BUSY_TIMEOUT"            : 30000
    , "BUSY_RETRY_LIMIT"        : 5
    , "WRITE_TURN_TIMEOUT"      : 600000
    })
    return this._STATIC_PARAMETERS;
  }

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#DBI_PARAMETERS
  }
  
  static get BUSY_TIMEOUT()               { return this.DBI_PARAMETERS.BUSY_TIMEOUT }
  static get BUSY_RETRY_LIMIT()           { return this.DBI_PARAMETERS.BUSY_RETRY_LIMIT }
  static get WRITE_TURN_TIMEOUT()         { return this.DBI_PARAMETERS.WRITE_TURN_TIMEOUT }

  static get IN_MEMORY_DATABASE()         { return ':memory:' }

  static get MAIN_SCHEMA()                { return 'main' }

  static get DATABASE_EXTENSION()         { return '.db' }

  static get STATEMENT_TERMINATOR()       { return ';' }

}

export { SqliteConstants as default }
