"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class MariadbConstants {

  static get DATABASE_KEY()               { return 'mariadb' };
  static get SOFTWARE_VENDOR()            { return 'MariaDB Corporation AB' };
  static get DATABASE_VENDOR()            { return 'MariaDB' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "TABLE_MATCHING"            : "INSENSITIVE"
    , "TREAT_TINYINT1_AS_BOOLEAN" : true    
    , "SPATIAL_FORMAT"            : "WKB"
	, "TIMESTAMP_PRECISION"       : 6
    })
    return this._STATIC_PARAMETERS;
  }
  
  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#_DBI_PARAMETERS
  }

  static get TABLE_MATCHING()             { return this.DBI_PARAMETERS.TABLE_MATCHING}
  static get TREAT_TINYINT1_AS_BOOLEAN()  { return this.DBI_PARAMETERS.TREAT_TINYINT1_AS_BOOLEAN}
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

  static get STAGED_DATA_SOURCES()    { return Object.freeze(['loader']) }

}

module.exports = MariadbConstants