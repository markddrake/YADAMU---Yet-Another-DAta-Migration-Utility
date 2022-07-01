
import YadamuConstants from '../../lib/yadamuConstants.js';

class DB2Constants {

  static get DATABASE_KEY()               { return 'db2' };
  static get DATABASE_VENDOR()            { return 'DB2' };
  static get SOFTWARE_VENDOR()            { return 'IBM Corporation' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
    })
    return this._STATIC_PARAMETERS;
  }

  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#_DBI_PARAMETERS
  }
  
  static get STATEMENT_TERMINATOR()       { return ';' }

  static get CLOSED_CONNECTION_ERROR() {
    this._CLOSED_CONNECTION_ERROR = this._CLOSED_CONNECTION_ERROR || Object.freeze(['S1000'])
    return this._CLOSED_CONNECTION_ERROR
  }

  static get LOST_CONNECTION_ERROR() {
    this._LOST_CONNECTION_ERROR = this._LOST_CONNECTION_ERROR || Object.freeze([-5005])
    return this._LOST_CONNECTION_ERROR
  }

}

export { DB2Constants as default }