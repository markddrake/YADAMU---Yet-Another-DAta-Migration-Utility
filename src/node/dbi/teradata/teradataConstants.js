"use strict"

import YadamuConstants from '../../lib/yadamuConstants.js';

class TeradataConstants {

  static get DATABASE_KEY()           { return 'teradata' };
  static get DATABASE_VENDOR()        { return 'Teradata' };
  static get SOFTWARE_VENDOR()        { return 'Teradata Inc' };
  static get STATEMENT_TERMINATOR()   { return ';' }
  static get STATEMENT_SEPERATOR()    { return '\n/\n' }


  static get STATIC_DEFAULTS() {
    this._STATIC_DEFAULTS = this._STATIC_DEFAULTS || Object.freeze({
	  BOOLEAN_STORAGE_OPTION      : "BYTEINT"
    })
    return this._STATIC_DEFAULTS;
 }

  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS() {
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_DEFAULTS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#_DBI_PARAMETERS
 }

  static get BOOLEAN_STORAGE_OPTION()              { return this.DBI_PARAMETERS.BOOLEAN_STORAGE_OPTION}
  
}

export { TeradataConstants as default }