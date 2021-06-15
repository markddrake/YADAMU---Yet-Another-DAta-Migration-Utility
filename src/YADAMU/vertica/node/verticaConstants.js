"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class VerticaConstants {

  static get DATABASE_KEY()           { return 'vertica' };
  static get DATABASE_VENDOR()        { return 'Vertica' };
  static get SOFTWARE_VENDOR()        { return 'Micro Focus International plc' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "SPATIAL_FORMAT"            : "WKB"
	, "COPY_TRIM_WHITEPSPACE"     : false
	, "MERGEOUT_INSERT_COUNT"     : 128
	, "BYTE_TO_CHAR_RATIO"        : 4
    })
    return this._STATIC_PARAMETERS;
  }

  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#_DBI_PARAMETERS
  }
  static get SPATIAL_FORMAT()         { return this.DBI_PARAMETERS.SPATIAL_FORMAT };
  static get COPY_TRIM_WHITEPSPACE()  { return this.DBI_PARAMETERS.COPY_TRIM_WHITEPSPACE === true };
  static get BYTE_TO_CHAR_RATIO()     { return this.DBI_PARAMETERS.BYTE_TO_CHAR_RATIO };
  static get MERGEOUT_INSERT_COUNT()  { return this.DBI_PARAMETERS.MERGEOUT_INSERT_COUNT };
  static get STATEMENT_TERMINATOR()   { return ';' }

  static get STAGED_DATA_SOURCES()    { return Object.freeze(['loader']) }

}

module.exports = VerticaConstants;