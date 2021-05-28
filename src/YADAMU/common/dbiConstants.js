"use strict"

const YadamuConstants = require('./yadamuConstants.js');

class DBIConstants {

  static get STATIC_PARAMETERS() {
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "MODE"                      : "DATA_ONLY"
    , "ON_ERROR"                  : "ABORT"
    , "SPATIAL_FORMAT"            : "WKB"
    , "TABLE_MAX_ERRORS"          : 10
    , "TOTAL_MAX_ERRORS"          : 100
    , "BATCH_SIZE"                : 10000
    , "COMMIT_RATIO"              : 1    
	, "INFINITY_MANAGEMENT"       : undefined
	, "LOCAL_STAGING_AREA"        : process.env.TMP || process.env.TEMP || process.platform === 'win32' ? "c:\\temp" : "/tmp"
	, "REMOTE_STAGING_AREA"       : process.env.TMP || process.env.TEMP || process.platform === 'win32' ? "c:\\temp" : "/tmp"
	, "STAGING_FILE_RETENTION"    : "FAILED"
    })
    return this._STATIC_PARAMETERS;
  }

  static get NEW_TIMINGS() {
    this._NEW_TIMINGS = this._NEW_TIMINGS || Object.freeze({
      rowsRead        : 0
    , pipeStartTime   : undefined
    , readerStartTime : undefined
    , readerEndTime   : undefined
	, parserStartTime : undefined
    , parserEndTime   : undefined
	, lost            : 0
	, failed          : false
    })
    return this._NEW_TIMINGS;
  }

  static #_YADAMU_DBI_PARAMETERS

  static get YADAMU_DBI_PARAMETERS()      { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuConstants.YADAMU_PARAMETERS,this.STATIC_PARAMETERS, YadamuConstants.YADAMU_CONFIGURATION.yadamuDBI))
	return this.#_YADAMU_DBI_PARAMETERS
  }
  
  static get SPATIAL_FORMAT()         { return this.YADAMU_DBI_PARAMETERS.SPATIAL_FORMAT };
  static get TABLE_MAX_ERRORS()       { return this.YADAMU_DBI_PARAMETERS.TABLE_MAX_ERRORS };
  static get TOTAL_MAX_ERRORS()       { return this.YADAMU_DBI_PARAMETERS.TOTAL_MAX_ERRORS };
  static get BATCH_SIZE()             { return this.YADAMU_DBI_PARAMETERS.BATCH_SIZE };
  static get COMMIT_RATIO()           { return this.YADAMU_DBI_PARAMETERS.COMMIT_RATIO };
  static get MODE()                   { return this.YADAMU_DBI_PARAMETERS.MODE }
  static get ON_ERROR()               { return this.YADAMU_DBI_PARAMETERS.ON_ERROR }
  static get INFINITY_MANAGEMENT()    { return this.YADAMU_DBI_PARAMETERS.INFINITY_MANAGEMENT };
  static get LOCAL_STAGING_AREA()     { return this.YADAMU_DBI_PARAMETERS.LOCAL_STAGING_AREA };
  static get REMOTE_STAGING_AREA()    { return this.YADAMU_DBI_PARAMETERS.REMOTE_STAGING_AREA };
  static get STAGING_FILE_RETENTION() { return this.YADAMU_DBI_PARAMETERS.STAGING_FILE_RETENTION };
  
}

module.exports = DBIConstants;