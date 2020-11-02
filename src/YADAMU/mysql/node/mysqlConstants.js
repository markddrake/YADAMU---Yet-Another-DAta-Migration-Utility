"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class MySQLConstants {

  static get MYSQL_DEFAULTS() { 
    this._MYSQL_DEFAULTS = this._MYSQL_DEFAULTS || Object.freeze({
      "TABLE_MATCHING"            : "INSENSITIVE"
    , "READ_KEEP_ALIVE"           : 0
    , "TREAT_TINYINT1_AS_BOOLEAN" : true    
    , "SPATIAL_FORMAT"            : "WKB"
    })
    return this._MYSQL_DEFAULTS;
  }

  static get DEFAULT_PARAMETERS() { 
    this._DEFAULT_PARAMETERS = this._DEFAULT_PARAMETERS || Object.freeze(Object.assign({},this.MYSQL_DEFAULTS,YadamuConstants.EXTERNAL_DEFAULTS.mysql || {}))
    return this._DEFAULT_PARAMETERS
  }

  static get TABLE_MATCHING()             { return this.DEFAULT_PARAMETERS.TABLE_MATCHING}
  static get READ_KEEP_ALIVE()            { return this.DEFAULT_PARAMETERS.READ_KEEP_ALIVE}
  static get TREAT_TINYINT1_AS_BOOLEAN()  { return this.DEFAULT_PARAMETERS.TREAT_TINYINT1_AS_BOOLEAN}
  static get SPATIAL_FORMAT()             { return this.DEFAULT_PARAMETERS.SPATIAL_FORMAT };
  static get DATABASE_VENDOR()            { return 'MySQL' };
  static get SOFTWARE_VENDOR()            { return 'Oracle Corporation (MySQL)' };
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

}

module.exports = MySQLConstants