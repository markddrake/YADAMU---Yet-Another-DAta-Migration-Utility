"use strict"

import YadamuConstants from '../../lib/yadamuConstants.js';

class DBIConstants {

  static get STATIC_PARAMETERS() {
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "MODE"                       : "DATA_ONLY"
    , "ON_ERROR"                   : "ABORT"
    , "SPATIAL_FORMAT"             : "WKB"
    , "TABLE_MAX_ERRORS"           : 10
    , "TOTAL_MAX_ERRORS"           : 100
    , "BATCH_SIZE"                 : 10000
    , "COMMIT_RATIO"               : 1    
	, "INFINITY_MANAGEMENT"        : undefined
	, "LOCAL_STAGING_AREA"         : process.env.TMP || process.env.TEMP || process.platform === 'win32' ? "c:\\temp" : "/tmp"
	, "REMOTE_STAGING_AREA"        : process.env.TMP || process.env.TEMP || process.platform === 'win32' ? "c:\\temp" : "/tmp"
	, "STAGING_FILE_RETENTION"     : "FAILED"
	, "TIMESTAMP_PRECISION"        : 9
	, "BYTE_TO_CHAR_RATIO"         : 1
    })
    return this._STATIC_PARAMETERS;
  }

  static get NEW_COPY_METRICS() {
	this._EMPTY_COPY_METRICS = this._EMPTY_COPY_METRICS || Object.freeze({
      pipeStartTime     : undefined
    , readerStartTime   : undefined
    , readerEndTime     : undefined
	, parserStartTime   : undefined
    , parserEndTime     : undefined
	, managerStartTime  : undefined
	, managerEndTime    : undefined
	, writerStartTime   : undefined
	, writerEndTime     : undefined
	, pipeEndTime       : undefined
	, failed            : false
	, readerError       : undefined
	, parserError       : undefined
	, managerError      : undefined
	, writerError       : undefined
	, read              : 0 // Rows read by the reader. Some Readers may not be able to supply a count. Cummulative
	, parsed            : 0 // Rows recieved he parser. Cummulative
	, received          : 0 // Rows encounted by the Output Manager. Cummulative
    , cached            : 0 // Rows cached in the current batch.
    , written           : 0 // Rows written to the database during the current transaction. Cummulative. Reset on each New Transaction
    , committed         : 0 // Rows written to the databsase and committed. Cummulative
    , skipped           : 0 // Rows not written due to unrecoverable write errors, eg Row is not valid per the target database. Includes any rows in-flight when a fatal error is reported
    , lost              : 0 // Rows written to the database but not yet committed when a rollback tooks place.
    , batchNumber       : 0 // Batch Number
	, pending           : 0 // Rows cached in batches that have not yet been written to disk
	, idleTime          : 0 // Time writer was not actively engaged in writing a batch E.g.Time between calls to processBatch()
    })
    return Object.assign({},this._EMPTY_COPY_METRICS);
  }

  static #_YADAMU_DBI_PARAMETERS

  static get YADAMU_DBI_PARAMETERS()      { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuConstants.YADAMU_PARAMETERS,this.STATIC_PARAMETERS, YadamuConstants.YADAMU_CONFIGURATION.yadamuDBI))
	return this.#_YADAMU_DBI_PARAMETERS
  }
  
  static get MODE()                       { return this.YADAMU_DBI_PARAMETERS.MODE }
  static get ON_ERROR()                   { return this.YADAMU_DBI_PARAMETERS.ON_ERROR }
  static get SPATIAL_FORMAT()             { return this.YADAMU_DBI_PARAMETERS.SPATIAL_FORMAT };
  static get TABLE_MAX_ERRORS()           { return this.YADAMU_DBI_PARAMETERS.TABLE_MAX_ERRORS };
  static get TOTAL_MAX_ERRORS()           { return this.YADAMU_DBI_PARAMETERS.TOTAL_MAX_ERRORS };
  static get BATCH_SIZE()                 { return this.YADAMU_DBI_PARAMETERS.BATCH_SIZE };
  static get COMMIT_RATIO()               { return this.YADAMU_DBI_PARAMETERS.COMMIT_RATIO };
  static get INFINITY_MANAGEMENT()        { return this.YADAMU_DBI_PARAMETERS.INFINITY_MANAGEMENT };
  static get LOCAL_STAGING_AREA()         { return this.YADAMU_DBI_PARAMETERS.LOCAL_STAGING_AREA };
  static get REMOTE_STAGING_AREA()        { return this.YADAMU_DBI_PARAMETERS.REMOTE_STAGING_AREA };
  static get STAGING_FILE_RETENTION()     { return this.YADAMU_DBI_PARAMETERS.STAGING_FILE_RETENTION };
  static get TIMESTAMP_PRECISION()        { return this.YADAMU_DBI_PARAMETERS.TIMESTAMP_PRECISION };
  static get BYTE_TO_CHAR_RATIO()         { return this.YADAMU_DBI_PARAMETERS.BYTE_TO_CHAR_RATIO };
  
  static get BATCH_WRITTEN()              { return 'batchSuccess' }
  static get BATCH_FAILED()               { return 'batchFailed' }
  static get BATCH_IDLE()                 { return 'batchIdle' }
  
}

export { DBIConstants as default}