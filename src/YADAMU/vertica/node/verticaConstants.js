"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class VerticaConstants {

  static get DATABASE_KEY()           { return 'vertica' };
  static get DATABASE_VENDOR()        { return 'Vertica' };
  static get SOFTWARE_VENDOR()        { return 'Micro Focus International plc' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "SPATIAL_FORMAT"            : "WKB"
	, "YADAMU_STAGING_FOLDER"     : process.env.TMP || process.ENV.TEMP || process.platform === 'win32' ? "c:\\temp" : "/tmp"
	, "VERTICA_STAGING_FOLDER"    : process.env.TMP || process.ENV.TEMP || process.platform === 'win32' ? "c:\\temp" : "/tmp"
	, "STAGING_FILE_RETENTION"    : "FAILED"
	, "VERTICA_CHAR_SIZE"         : 4
    })
    return this._STATIC_PARAMETERS;
  }

  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#_DBI_PARAMETERS
  }

  static get PGOID_DATE()          { return _PGOID_DATE }
  static get PGOID_TIMESTAMP()     { return _PGOID_TIMESTAMP }
  static get PGOID_TIMESTAMP_TZ()  { return _PGOID_TIMESTAMP_TZ }

  static get FETCH_AS_STRING() { 
    this._FETCH_AS_STRING = this._FETCH_AS_STRING || Object.freeze([this.PGOID_DATE,this.PGOID_TIMESTAMP,this.PGOID_TIMESTAMP_TZ])
    return this._FETCH_AS_STRING;
  }

  static get SPATIAL_FORMAT()         { return this.DBI_PARAMETERS.SPATIAL_FORMAT };
  static get YADAMU_STAGING_FOLDER()  { return this.DBI_PARAMETERS.YADMAU_STAGING_FOLDER };
  static get VERTICA_STAGING_FOLDER() { return this.DBI_PARAMETERS.VERTICA_STAGING_FOLDER };
  static get STAGING_FILE_RETENTION() { return this.DBI_PARAMETERS.STAGING_FILE_RETENTION };
  static get VERTICA_CHAR_SIZE()      { return this.DBI_PARAMETERS.VERTICA_CHAR_SIZE };
  static get STATEMENT_TERMINATOR()   { return ';' }


}

module.exports = VerticaConstants;