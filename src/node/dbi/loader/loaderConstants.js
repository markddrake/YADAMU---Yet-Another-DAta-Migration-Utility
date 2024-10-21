
import YadamuConstants from '../../lib/yadamuConstants.js';

class LoaderConstants {

  static get DATABASE_KEY()      { return 'loader' };
  static get DATABASE_VENDOR()   { return 'YABASC' };
  static get SOFTWARE_VENDOR()   { return 'YABASC - Yet Another Bay Area Software Compsny'};
  static get PROTOCOL()          { return 'file://' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({})
    return this._STATIC_PARAMETERS;
  }

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#DBI_PARAMETERS
  }
     
}

export {LoaderConstants as default }