"use strict"

import YadamuConstants from '../../lib/yadamuConstants.js';

class MariadbConstants {

  static get DATABASE_KEY()               { return 'mariadb' };
  static get SOFTWARE_VENDOR()            { return 'MariaDB Corporation AB' };
  static get DATABASE_VENDOR()            { return 'MariaDB' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "TREAT_TINYINT1_AS_BOOLEAN" : true 
    , "CHAR_LENGTH"               : 255
	, "BINARY_LENGTH"             : 255
   	, "VARCHAR_LENGTH"            : 4096
	, "VARBINARY_LENGTH"          : 8192
	, "BIT_LENGTH"                : 64
	, "NUMERIC_PRECISION"         : 65
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

  static get TREAT_TINYINT1_AS_BOOLEAN()  { return this.DBI_PARAMETERS.TREAT_TINYINT1_AS_BOOLEAN}
  static get SPATIAL_FORMAT()             { return this.DBI_PARAMETERS.SPATIAL_FORMAT };
  static get CHAR_LENGTH()                { return this.DBI_PARAMETERS.CHAR_LENGTH}
  static get BINARY_LENGTH()              { return this.DBI_PARAMETERS.BINARY_LENGTH}
  static get VARCHAR_LENGTH()             { return this.DBI_PARAMETERS.VARCHAR_LENGTH}
  static get VARBINARY_LENGTH()           { return this.DBI_PARAMETERS.VARBINARY_LENGTH}
  static get BIT_LENGTH()                 { return this.DBI_PARAMETERS.BIT_LENGTH}
  static get NUMERIC_PRECISION()          { return this.DBI_PARAMETERS.NUMERIC_PRECISION }


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