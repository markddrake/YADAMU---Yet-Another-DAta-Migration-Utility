"use strict"

const YadamuConstants = require('./yadamuConstants.js');

class DBIConstants {

  static get YADAMU_DBI_DEFAULTS() {
    this._YADAMU_DBI_DEFAULTS = this._YADAMU_DBI_DEFAULTS || Object.freeze({
      "SPATIAL_FORMAT"            : "WKB"
    , "TABLE_MAX_ERRORS"          : 10
    , "TOTAL_MAX_ERRORS"          : 100
    , "BATCH_SIZE"                : 10000
    , "COMMIT_RATIO"              : 1    
    })
    return this._YADAMU_DBI_DEFAULTS;
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

  static get DEFAULT_PARAMETERS() { 
    this._DEFAULT_PARAMETERS = this._DEFAULT_PARAMETERS || Object.freeze(Object.assign({},this.YADAMU_DBI_DEFAULTS, YadamuConstants.EXTERNAL_DEFAULTS.yadamuDBI))
    return this._DEFAULT_PARAMETERS
  }

  static get SPATIAL_FORMAT()      { return this.DEFAULT_PARAMETERS.SPATIAL_FORMAT };
  static get TABLE_MAX_ERRORS()    { return this.DEFAULT_PARAMETERS.TABLE_MAX_ERRORS };
  static get TOTAL_MAX_ERRORS()    { return this.DEFAULT_PARAMETERS.TOTAL_MAX_ERRORS };
  static get BATCH_SIZE()          { return this.DEFAULT_PARAMETERS.BATCH_SIZE };
  static get COMMIT_RATIO()        { return this.DEFAULT_PARAMETERS.COMMIT_RATIO };
  
}

module.exports = DBIConstants;