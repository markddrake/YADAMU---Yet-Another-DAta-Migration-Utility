
import YadamuConstants from '../../lib/yadamuConstants.js';

class DBIConstants {

  static get STATIC_PARAMETERS() {
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "MODE"                       : "DATA_ONLY"
    , "ON_ERROR"                   : "ABORT"
    , "TABLE_MAX_ERRORS"           : 10
    , "TOTAL_MAX_ERRORS"           : 100
    , "BATCH_SIZE"                 : 10000
    , "BATCH_LIMIT"                : 5
    , "COMMIT_RATIO"               : 1    
	, "INFINITY_MANAGEMENT"        : "REJECT"
	, "LOCAL_STAGING_AREA"         : process.env.TMP || process.env.TEMP || process.platform === 'win32' ? "c:\\temp" : "/tmp"
	, "REMOTE_STAGING_AREA"        : process.env.TMP || process.env.TEMP || process.platform === 'win32' ? "c:\\temp" : "/tmp"
	, "STAGING_FILE_RETENTION"     : "FAILED"
	, "BYTE_TO_CHAR_RATIO"         : 1
    })
    return this._STATIC_PARAMETERS;
  }

  static get INPUT_STREAM_ID()           { return 'InputStream'  }
  static get PARSER_STREAM_ID()          { return 'Parser'       }
  static get TRANSFORMATION_STREAM_ID()  { return 'Transformer'  }
  static get OUTPUT_STREAM_ID()         { return 'OutputStream' }
  
  static get PIPELINE_STATE() {
	this._EMPTY_PIPELINE_STATE = this._EMPTY_PIPELINE_STATE || Object.freeze({
	  failed            : false
    , startTime     : undefined
	, endTime       : undefined
	, read              : 0 // Rows read by the reader. Some Readers may not be able to supply a count. Cummulative
	, parsed            : 0 // Rows recieved he parser. Cummulative
	, received          : 0 // Rows encounted by the Output Manager. Cummulative
    , committed         : 0 // Rows written to the databsase and committed. Cummulative
    , cached            : 0 // Rows cached in the current batch. Maintianed by the Output Manager
	, pending           : 0 // Rows cached in batches that have not yet been written to disk. Incremented by the Output Manager when pushing a batch and decremented by Writer when a batch is written.
    , written           : 0 // Rows written to the database during the current transaction. Cummulative. Reset on each New Transaction
    , skipped           : 0 // Rows not written due to unrecoverable write errors, eg Row is not valid per the target database. Includes any rows in-flight when a fatal error is reported
    , lost              : 0 // Rows written to the database but not yet committed when a rollback takes place or a write connection is lost.
	, idleTime          : 0 // Time writer was not actively engaged in writing a batch E.g.Time between calls to processBatch()
    , batchNumber       : 0 // Batch Number
    , batchWritten      : 0 // Batches Writen 

    })
    return Object.assign({},this._EMPTY_PIPELINE_STATE);
  }
  
  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS()      { 
	this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuConstants.YADAMU_PARAMETERS,this.STATIC_PARAMETERS, YadamuConstants.YADAMU_CONFIGURATION.yadamuDBI))
	return this.#_DBI_PARAMETERS
  }
  
  static get MODE()                       { return this.DBI_PARAMETERS.MODE }

  static get ON_ERROR()                   { return this.DBI_PARAMETERS.ON_ERROR }
  static get TABLE_MAX_ERRORS()           { return this.DBI_PARAMETERS.TABLE_MAX_ERRORS };
  static get TOTAL_MAX_ERRORS()           { return this.DBI_PARAMETERS.TOTAL_MAX_ERRORS };
  static get BATCH_SIZE()                 { return this.DBI_PARAMETERS.BATCH_SIZE };
  static get BATCH_LIMIT()                { return this.DBI_PARAMETERS.BATCH_LIMIT };
  static get COMMIT_RATIO()               { return this.DBI_PARAMETERS.COMMIT_RATIO };
  static get INFINITY_MANAGEMENT()        { return this.DBI_PARAMETERS.INFINITY_MANAGEMENT };
  static get LOCAL_STAGING_AREA()         { return this.DBI_PARAMETERS.LOCAL_STAGING_AREA };
  static get REMOTE_STAGING_AREA()        { return this.DBI_PARAMETERS.REMOTE_STAGING_AREA };
  static get STAGING_FILE_RETENTION()     { return this.DBI_PARAMETERS.STAGING_FILE_RETENTION };
  static get BYTE_TO_CHAR_RATIO()         { return this.DBI_PARAMETERS.BYTE_TO_CHAR_RATIO };
  
  static get BATCH_COMPLETED()            { return 'batchCompleted' }
  static get BATCH_RELEASED()             { return 'batchReleased' }
  static get BATCH_WRITTEN()              { return 'batchSuccess' }
  static get BATCH_FAILED()               { return 'batchFailed' }
  static get BATCH_IDLE()                 { return 'batchIdle' }

  static #_STAGING_UNSUPPORTED

  static get STAGING_UNSUPPORTED()        { 
     this.#_STAGING_UNSUPPORTED  = this.#_STAGING_UNSUPPORTED || Object.freeze([]) 
	 return this.#_STAGING_UNSUPPORTED
  }
  
  static #_LOADER_STAGING

  static get LOADER_STAGING()             { 
     this.#_LOADER_STAGING  = this.#_LOADER_STAGING || Object.freeze(['loader'])
	 return this.#_LOADER_STAGING
  }

  static #_CLOUD_STAGING

  static get CLOUD_STAGING()             { 
     this.#_CLOUD_STAGING  = this.#_CLOUD_STAGING || Object.freeze(['awsS3','azure'])
	 return this.#_CLOUD_STAGING
  }
 
 
}

export { DBIConstants as default}