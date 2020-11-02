"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class SnowflakeConstants {

  static get SNOWFLAKE_DEFAULTS()      { 
    this._SNOWFLAKE_DEFAULTS = this._SNOWFLAKE_DEFAULTS || Object.freeze({
      "YADAMU_USER"              : "public"
    , "MAX_CHARACTER_SIZE"        : 16777216
	, "MAX_BINARY_SIZE"           : 8388608 
    , "TRANSIENT_TABLES"          : true
    , "DATA_RETENTION_TIME"       : 0
    , "SPATIAL_FORMAT"            : "WKB"
    })
    return this._SNOWFLAKE_DEFAULTS;
  }

  static get DEFAULT_PARAMETERS() { 
    this._DEFAULT_PARAMETERS = this._DEFAULT_PARAMETERS || Object.freeze(Object.assign({},this.SNOWFLAKE_DEFAULTS,YadamuConstants.EXTERNAL_DEFAULTS.snowflake))
    return this._DEFAULT_PARAMETERS
  }

  static get YADAMU_USER()           { return this.DEFAULT_PARAMETERS.YADAMU_USER}
  static get MAX_CHARACTER_SIZE()     { return this.DEFAULT_PARAMETERS.MAX_CHARACTER_SIZE}
  static get MAX_BINARY_SIZE()        { return this.DEFAULT_PARAMETERS.MAX_BINARY_SIZE}
  static get TRANSIENT_TABLES()       { return this.DEFAULT_PARAMETERS.TRANSIENT_TABLES}
  static get DATA_RETENTION_TIME()    { return this.DEFAULT_PARAMETERS.DATA_RETENTION_TIME}
  static get SPATIAL_FORMAT()         { return this.DEFAULT_PARAMETERS.SPATIAL_FORMAT };
  static get DATABASE_VENDOR()        { return 'SNOWFLAKE' };
  static get SOFTWARE_VENDOR()        { return 'Snowflake Software Inc' };
  static get STATEMENT_TERMINATOR()   { return ';' }

  static get VARIANT_DATA_TYPE()      { return `VARIANT` }
  static get XML_TYPE()               { return this.VARIANT_DATA_TYPE }  
  static get JSON_TYPE()              { return this.VARIANT_DATA_TYPE }
  static get CLOB_TYPE()              { return `VARCHAR(${this.MAX_CHARACTER_SIZE})`}
  static get BLOB_TYPE()              { return `BINARY(${this.MAX_BINARY_SIZE})`}
  
  static get LOST_CONNECTION_ERROR() {
    this._LOST_CONNECTION_ERROR = this._LOST_CONNECTION_ERROR || Object.freeze([407002])
    return this._LOST_CONNECTION_ERROR
  }

  static get LOST_CONNECTION_STATE() {
    this._LOST_CONNECTION_STATE = this._LOST_CONNECTION_STATE || Object.freeze(['08003',])
    return this._LOST_CONNECTION_STATE
  }

  static get SERVER_UNAVAILABLE_ERROR() {
	return this.LOST_CONNECTION_ERROR()
  }

  static get SERVER_UNAVAILABLE_STATE() {
	return this.LOST_CONNECTION_STATE()
  }


}

module.exports = SnowflakeConstants