"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class MsSQLConstants {
    
  static get MsSQL_DEFAULTS() { 
    this._MsSQL_DEFAULTS = this._MsSQL_DEFAULTS || Object.freeze({
      "YADAMU_USER"              : "dbo"
    , "SPATIAL_MAKE_VALID"        : false
    , "SPATIAL_FORMAT"            : "WKB"
    })
    return this._MsSQL_DEFAULTS;
  }

  static get DEFAULT_PARAMETERS() { 
    this._DEFAULT_PARAMETERS = this._DEFAULT_PARAMETERS || Object.freeze(Object.assign({},this.MsSQL_DEFAULTS,YadamuConstants.EXTERNAL_DEFAULTS.mssql || {}))
    return this._DEFAULT_PARAMETERS
  }

  static get YADAMU_USER()           { return this.DEFAULT_PARAMETERS.YADAMU_USER}
  static get SPATIAL_FORMAT()         { return this.DEFAULT_PARAMETERS.SPATIAL_FORMAT };
  static get SPATIAL_MAKE_VALID()     { return this.DEFAULT_PARAMETERS.SPATIAL_MAKE_VALID };
  static get DATABASE_VENDOR()        { return 'MSSQLSERVER' };
  static get SOFTWARE_VENDOR()        { return 'Microsoft Corporation' };
  static get STATEMENT_TERMINATOR()   { return 'go' }
 
  static get STAGING_TABLE () { 
    this._STAGING_TABLE = this._STAGING_TABLE || Object.freeze({
      tableName  : '#YADAMU_STAGING'
    , columnName : 'DATA'    
    })
    return this._STAGING_TABLE
  }
  
  static get MISSING_TABLE_ERROR() {
    this._MISSING_TABLE_ERROR = this._MISSING_TABLE_ERROR || Object.freeze([208])
    return this._MISSING_TABLE_ERROR
  }

  static get LOST_CONNECTION_ERROR() {
    this._LOST_CONNECTION_ERROR = this._LOST_CONNECTION_ERROR || Object.freeze(['ETIMEOUT','ESOCKET','EINVALIDSTATE','ECONNRESET','ECONNCLOSED'])
    return this._LOST_CONNECTION_ERROR
  }

  static get INVALID_CONNECTION_ERROR() {
    this._INVALID_CONNECTION_ERROR = this._INVALID_CONNECTION_ERROR || Object.freeze(['EINVALIDSTATE'])
    return this._INVALID_CONNECTION_ERROR
  }

  static get SERVER_UNAVAILABLE_ERROR() {
    this._SERVER_UNAVAILABLE_ERROR = this._SERVER_UNAVAILABLE_ERROR || Object.freeze(['ETIMEOUT','ESOCKET'])
    return this._SERVER_UNAVAILABLE_ERROR
  }

  static get SPATIAL_ERROR() {
    this._SPATIAL_ERROR = this._SPATIAL_ERROR || Object.freeze([])
    return this._SPATIAL_ERROR
  }

  static get JSON_PARSING_ERROR() {
    this._JSON_PARSING_ERROR = this._JSON_PARSING_ERROR || Object.freeze([])
    return this._JSON_PARSING_ERROR
  }
  
  static get SUPPRESSED_ERROR() {
	this._SUPPRESSED_ERROR = this._SUPPRESSED_ERROR || Object.freeze([
   	  `Received 'row' when no sqlRequest is in progress`,
	  `No event 'data' in state 'Final'`
    ])
	return this._SUPPRESSED_ERROR
  }

}

module.exports = MsSQLConstants