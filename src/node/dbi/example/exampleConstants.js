"use strict"

import YadamuConstants from '../../lib/yadamuConstants.js';

class ExampleConstants {

  static get DATABASE_KEY()               { return 'example' };
  static get DATABASE_VENDOR()            { return 'Example' };
  static get SOFTWARE_VENDOR()            { return 'Example Corporation' };

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

}

export { ExampleConstants as default }