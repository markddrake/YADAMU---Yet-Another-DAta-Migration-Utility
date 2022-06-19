
import YadamuConstants from '../../lib/yadamuConstants.js';

class SnowflakeConstants {

  static get DATABASE_KEY()           { return 'snowflake' };
  static get DATABASE_VENDOR()        { return 'SNOWFLAKE' };
  static get SOFTWARE_VENDOR()        { return 'Snowflake Software Inc' };
  static get VARIANT_DATA_TYPE()      { return `VARIANT` }

  static get STATIC_PARAMETERS()      { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "TRANSIENT_TABLES"          : true
    , "DATA_RETENTION_TIME"       : 0
	, "XML_STORAGE_OPTION"        : this.VARIANT_DATA_TYPE
	, "JSON_STORAGE_OPTION"       : this.VARIANT_DATA_TYPE
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

  static get TRANSIENT_TABLES()       { return this.DBI_PARAMETERS.TRANSIENT_TABLES}
  static get DATA_RETENTION_TIME()    { return this.DBI_PARAMETERS.DATA_RETENTION_TIME}

  static get XML_STORAGE_OPTION()     { return this.DBI_PARAMETERS.XML_STORAGE_OPTION };
  static get JSON_STORAGE_OPTION()    { return this.DBI_PARAMETERS.JSON_STORAGE_OPTION };

  static get STATEMENT_TERMINATOR()   { return ';' }

  static get LOST_CONNECTION_ERROR() {
    this._LOST_CONNECTION_ERROR = this._LOST_CONNECTION_ERROR || Object.freeze(['407002','401001'])
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

  static get CONTENT_TOO_LARGE_ERROR() {
	/*  errorCode: '100078', sqlState: '22000' */
    this._CONTENT_TOO_LARGE_ERROR = this._CONTENT_TOO_LARGE_ERROR || Object.freeze(['100078'])
    return this._CONTENT_TOO_LARGE_ERROR
  }
  
  static get CONTENT_TOO_LARGE_STATE() {
	/*  errorCode: '100078', sqlState: '22000' */
    this._CONTENT_TOO_LARGE_STATE = this._CONTENT_TOO_LARGE_STATE || Object.freeze(['22000'])
    return this._CONTENT_TOO_LARGE_STATE
  }


}

export { SnowflakeConstants as default }