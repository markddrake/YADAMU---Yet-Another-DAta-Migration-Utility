"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class MySQLConstants {

  static get DATABASE_KEY()               { return 'mysql' };
  static get DATABASE_VENDOR()            { return 'MySQL' };
  static get SOFTWARE_VENDOR()            { return 'Oracle Corporation (MySQL)' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "TABLE_MATCHING"            : "INSENSITIVE"
    , "READ_KEEP_ALIVE"           : 0
    , "TREAT_TINYINT1_AS_BOOLEAN" : true    
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

  static get TABLE_MATCHING()             { return this.DBI_PARAMETERS.TABLE_MATCHING}
  static get READ_KEEP_ALIVE()            { return this.DBI_PARAMETERS.READ_KEEP_ALIVE}
  static get TREAT_TINYINT1_AS_BOOLEAN()  { return this.DBI_PARAMETERS.TREAT_TINYINT1_AS_BOOLEAN}
  static get SPATIAL_FORMAT()             { return this.DBI_PARAMETERS.SPATIAL_FORMAT };
  static get STATEMENT_TERMINATOR()       { return ';' }
 
  static get CONNECTION_PROPERTY_DEFAULTS() { 
    this._CONNECTION_PROPERTY_DEFAULTS = this._CONNECTION_PROPERTY_DEFAULTS || Object.freeze({
      multipleStatements: true
    , typeCast          : true
    , supportBigNumbers : true
    , bigNumberStrings  : true          
    , dateStrings       : true
    , trace             : true
    })
   return this._CONNECTION_PROPERTY_DEFAULTS;
  }

  static get MISSING_TABLE_ERROR() {
    this._MISSING_TABLE_ERROR = this._MISSING_TABLE_ERROR || Object.freeze(['ER_NO_SUCH_TABLE'])
    return this._MISSING_TABLE_ERROR
  }

  static get LOST_CONNECTION_ERROR() {
    this._LOST_CONNECTION_ERROR = this._LOST_CONNECTION_ERROR || Object.freeze(['ECONNRESET','PROTOCOL_CONNECTION_LOST','ER_CMD_CONNECTION_CLOSED','ER_SOCKET_UNEXPECTED_CLOSE','ER_GET_CONNECTION_TIMEOUT','PROTOCOL_ENQUEUE_AFTER_FATAL_ERROR'])
    return this._LOST_CONNECTION_ERROR
  }

  static get SERVER_UNAVAILABLE_ERROR() {
    this._SERVER_UNAVAILABLE_ERROR = this._SERVER_UNAVAILABLE_ERROR || Object.freeze(['ECONNREFUSED','ER_GET_CONNECTION_TIMEOUT'])
    return this._SERVER_UNAVAILABLE_ERROR
  }

  static get SPATIAL_ERROR() {
    this._SPATIAL_ERROR = this._SPATIAL_ERROR || Object.freeze(['ER_GIS_INVALID_DATA'])
    return this._SPATIAL_ERROR
  }

  static get UNKNOWN_CODE_ERROR() {
    this._UNKNOWN_CODE_ERROR = this._UNKNOWN_CODE_ERROR || Object.freeze(['UNKNOWN_CODE_PLEASE_REPORT'])
    return this._UNKNOWN_CODE_ERROR
  }

  static get JSON_PARSING_ERROR() {
    this._JSON_PARSING_ERROR = this._JSON_PARSING_ERROR || Object.freeze([])
    return this._JSON_PARSING_ERROR
  }

  static get STAGED_DATA_SOURCES()    { return Object.freeze(['loader']) }
}

module.exports = MySQLConstants