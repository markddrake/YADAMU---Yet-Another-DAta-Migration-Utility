"use strict"

import YadamuConstants from '../../common/yadamuConstants.js';

class SnowflakeConstants {

  static get DATABASE_KEY()           { return 'snowflake' };
  static get DATABASE_VENDOR()        { return 'SNOWFLAKE' };
  static get SOFTWARE_VENDOR()        { return 'Snowflake Software Inc' };
  static get VARIANT_DATA_TYPE()      { return `VARIANT` }

  static get STATIC_PARAMETERS()      { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "YADAMU_USER"              : "public"
    , "MAX_CHARACTER_SIZE"        : 16777216
	, "MAX_BINARY_SIZE"           : 8388608 
    , "TRANSIENT_TABLES"          : true
    , "DATA_RETENTION_TIME"       : 0
    , "SPATIAL_FORMAT"            : "WKB"
	, "SNOWFLAKE_XML_TYPE"        : this.VARIANT_DATA_TYPE
	, "SNOWFLAKE_JSON_TYPE"       : this.VARIANT_DATA_TYPE
    })
    return this._STATIC_PARAMETERS;
  }
  
  static get TIME_INPUT_FORMAT() {
    this._TIMESTAMP_FORMAT_MASKS = this._TIMESTAMP_FORMAT_MASKS || Object.freeze({
      Oracle      : 'HH24:MI:SS.FF9'
    , MSSQLSERVER : 'YYYY-MM-DDTHH24:MI:SS'
    , Postgres    : 'YYYY-MM-DDTHH24:MI:SS.FF6'
    , Vertica     : 'HH24:MI:SS.FF9'
    , MySQL       : 'YYYY-MM-DDTHH24:MI:SS.FF6TZH:TZM'
    , MariaDB     : 'YYYY-MM-DDTHH24:MI:SS.FF6TZH:TZM'
    , MongoDB     : 'HH24:MI:SS.FF9'
    , SNOWFLAKE   : 'YYYY-MM-DDTHH24:MI:SS.FF9TZH:TZM'
    })
    return this._TIMESTAMP_FORMAT_MASKS
  }
  static #_DBI_PARAMETERS
  
  static get DBI_PARAMETERS() { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS1: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
	return this.#_DBI_PARAMETERS
  }

  static get YADAMU_USER()            { return this.DBI_PARAMETERS.YADAMU_USER}
  static get MAX_CHARACTER_SIZE()     { return this.DBI_PARAMETERS.MAX_CHARACTER_SIZE}
  static get MAX_BINARY_SIZE()        { return this.DBI_PARAMETERS.MAX_BINARY_SIZE}
  static get TRANSIENT_TABLES()       { return this.DBI_PARAMETERS.TRANSIENT_TABLES}
  static get DATA_RETENTION_TIME()    { return this.DBI_PARAMETERS.DATA_RETENTION_TIME}
  static get SPATIAL_FORMAT()         { return this.DBI_PARAMETERS.SPATIAL_FORMAT };
  static get SNOWFLAKE_XML_TYPE()     { return this.DBI_PARAMETERS.SNOWFLAKE_XML_TYPE };
  static get SNOWFLAKE_JSON_TYPE()    { return this.DBI_PARAMETERS.SNOWFLAKE_JSON_TYPE };
  static get STATEMENT_TERMINATOR()   { return ';' }

  static get CLOB_TYPE()              { return `VARCHAR(${this.MAX_CHARACTER_SIZE})`}
  static get BLOB_TYPE()              { return `BINARY(${this.MAX_BINARY_SIZE})`}
  
  static get LOST_CONNECTION_ERROR() {
    this._LOST_CONNECTION_ERROR = this._LOST_CONNECTION_ERROR || Object.freeze([407002,401001])
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

  static get STAGED_DATA_SOURCES()    { return Object.freeze(['awsS3']) }

}

export { SnowflakeConstants as default }