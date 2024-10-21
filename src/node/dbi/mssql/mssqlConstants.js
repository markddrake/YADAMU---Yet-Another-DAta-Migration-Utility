
import YadamuConstants from '../../lib/yadamuConstants.js';

class MsSQLConstants {

  static get DATABASE_KEY()           { return 'mssql' };
  static get DATABASE_VENDOR()        { return 'MSSQLSERVER' };
  static get SOFTWARE_VENDOR()        { return 'Microsoft Corporation' };

  static get REQUEST_CANCELLED()      { return 'cancelationComplete' };

    
  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "SPATIAL_MAKE_VALID"        : false
  , "ROW_LIMIT"                 : 8060
    })
    return this._STATIC_PARAMETERS;
  }

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#DBI_PARAMETERS
  }

  static get SPATIAL_MAKE_VALID()     { return this.DBI_PARAMETERS.SPATIAL_MAKE_VALID };
  static get ROW_LIMIT()              { return this.DBI_PARAMETERS.ROW_LIMIT };

  static get STATEMENT_TERMINATOR()   { return ';' }
 
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

  static get CONTENT_TOO_LARGE_ERROR() {
    this._CONTENT_TOO_LARGE_ERROR = this._CONTENT_TOO_LARGE_ERROR || Object.freeze([511])
    return this._CONTENT_TOO_LARGE_ERROR
  }

  static get LOST_CONNECTION_ERROR() {
    this._LOST_CONNECTION_ERROR = this._LOST_CONNECTION_ERROR || Object.freeze(['ETIMEOUT','ESOCKET','EINVALIDSTATE','ECONNRESET','ECONNCLOSED'])
    return this._LOST_CONNECTION_ERROR
  }

  static get INVALID_STATE_ERROR() {
    this.INVALID_STATE_ERROR = this.INVALID_STATE_ERROR || Object.freeze(['EINVALIDSTATE'])
    return this.INVALID_STATE_ERROR
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

export { MsSQLConstants as default }