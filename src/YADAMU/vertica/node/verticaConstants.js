"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class VerticaConstants {

  static get DATABASE_KEY()           { return 'vertica' };
  static get DATABASE_VENDOR()        { return 'Vertica' };
  static get SOFTWARE_VENDOR()        { return 'Micro Focus International plc' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "SPATIAL_FORMAT"            : "WKB"
	, "YADAMU_STAGING_FOLDER"     : process.env.TMP || process.env.TEMP || process.platform === 'win32' ? "c:\\temp" : "/tmp"
	, "VERTICA_STAGING_FOLDER"    : process.env.TMP || process.env.TEMP || process.platform === 'win32' ? "c:\\temp" : "/tmp"
	, "STAGING_FILE_RETENTION"    : "FAILED"
	, "PRESERVE_WHITESPACE"       : true
	, "MERGEOUT_INSERT_COUNT"     : 1024
	, "VERTICA_CHAR_SIZE"         : 4
    })
    return this._STATIC_PARAMETERS;
  }

  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#_DBI_PARAMETERS
  }
  static get SPATIAL_FORMAT()         { return this.DBI_PARAMETERS.SPATIAL_FORMAT };
  static get YADAMU_STAGING_FOLDER()  { return this.DBI_PARAMETERS.YADMAU_STAGING_FOLDER };
  static get VERTICA_STAGING_FOLDER() { return this.DBI_PARAMETERS.VERTICA_STAGING_FOLDER };
  static get STAGING_FILE_RETENTION() { return this.DBI_PARAMETERS.STAGING_FILE_RETENTION };
  static get PRESERVE_WHITESPACE()    { return this.DBI_PARAMETERS.PRESERVE_WHITESPACE === true };
  static get VERTICA_CHAR_SIZE()      { return this.DBI_PARAMETERS.VERTICA_CHAR_SIZE };
  static get MERGEOUT_INSERT_COUNT()  { return this.DBI_PARAMETERS.MERGEOUT_INSERT_COUNT };
  static get STATEMENT_TERMINATOR()   { return ';' }


}

module.exports = VerticaConstants;