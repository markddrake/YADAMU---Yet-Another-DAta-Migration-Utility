"use strict"

import YadamuConstants from '../../lib/yadamuConstants.js';

class MariadbConstants {

  static get DATABASE_KEY()               { return 'mariadb' };
  static get SOFTWARE_VENDOR()            { return 'MariaDB Corporation AB' };
  static get DATABASE_VENDOR()            { return 'MariaDB' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "SPATIAL_FORMAT"            : "WKB"
    })
    return this._STATIC_PARAMETERS;
  }
  
  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#_DBI_PARAMETERS
  }

  static get SPATIAL_FORMAT()             { return this.DBI_PARAMETERS.SPATIAL_FORMAT };
  static get STATEMENT_TERMINATOR()       { return ';' }
 
  
  static get CONNECTION_PROPERTY_DEFAULTS() { 
    this._CONNECTION_PROPERTY_DEFAULTS = this._CONNECTION_PROPERTY_DEFAULTS || Object.freeze({
      multipleStatements: true
    , typeCast          : true
    , supportBigNumbers : true
    , bigNumberStrings  : true          
    , dateStrings       : true
    , rowsAsArray       : true
    , trace             : true
    })
   return this._CONNECTION_PROPERTY_DEFAULTS;
  }

}

export { MariadbConstants as default }