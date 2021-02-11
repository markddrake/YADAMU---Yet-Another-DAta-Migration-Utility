"use strict"

const oracledb = require('oracledb');

const YadamuConstants = require('../../common/yadamuConstants.js');

class OracleConstants {

  static get DATABASE_KEY()           { return 'oracle' };
  static get DATABASE_VENDOR()        { return 'Oracle' };
  static get SOFTWARE_VENDOR()        { return 'Oracle Corporation' };
  static get STATEMENT_TERMINATOR()   { return '/' }

  static get STATIC_DEFAULTS() { 
    this._STATIC_DEFAULTS = this._STATIC_DEFAULTS || Object.freeze({
      "BATCH_LOB_COUNT"           : 1024
    , "LOB_MIN_SIZE"              : 32768
    , "LOB_MAX_SIZE"              : 16777216
    , "LOB_CACHE_COUNT"           : 50000
    , "XML_STORAGE_FORMAT"        : "XML"
    , "JSON_STORAGE_FORMAT"       : "JSON"
    , "MIGRATE_JSON_STORAGE"      : false
    , "OBJECTS_AS_JSON"           : false
    , "TREAT_RAW1_AS_BOOLEAN"     : true
    , "SPATIAL_FORMAT"            : "WKB"
    })
    return this._STATIC_DEFAULTS;
  }

  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_DEFAULTS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#_DBI_PARAMETERS
  }

  static get BATCH_LOB_COUNT()        { return this.DBI_PARAMETERS.BATCH_LOB_COUNT}
  static get LOB_MIN_SIZE()           { return this.DBI_PARAMETERS.LOB_MIN_SIZE}
  static get LOB_MAX_SIZE()           { return this.DBI_PARAMETERS.LOB_MAX_SIZE}
  static get LOB_CACHE_COUNT()        { return this.DBI_PARAMETERS.LOB_CACHE_COUNT}
  static get XML_STORAGE_FORMAT()     { return this.DBI_PARAMETERS.XML_STORAGE_FORMAT}
  static get JSON_STORAGE_FORMAT()    { return this.DBI_PARAMETERS.JSON_STORAGE_FORMAT}
  static get MIGRATE_JSON_STORAGE()   { return this.DBI_PARAMETERS.MIGRATE_JSON_STORAGE}
  static get OBJECTS_AS_JSON()        { return this.DBI_PARAMETERS.OBJECTS_AS_JSON}
  static get TREAT_RAW1_AS_BOOLEAN()  { return this.DBI_PARAMETERS.TREAT_RAW1_AS_BOOLEAN}
  static get SPATIAL_FORMAT()         { return this.DBI_PARAMETERS.SPATIAL_FORMAT };
 
  // Until we have static constants

  static get LOB_STRING_MAX_LENGTH()   { return _LOB_STRING_MAX_LENGTH }
  static get BFILE_STRING_MAX_LENGTH() { return _BFILE_STRING_MAX_LENGTH }
  static get STRING_MAX_LENGTH()       { return _STRING_MAX_LENGTH }

  static get DATE_FORMAT_MASKS() { 
    this._DATE_FORMAT_MASKS = this._DATE_FORMAT_MASKS || Object.freeze({
      Oracle      : 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
    , MSSQLSERVER : 'YYYY-MM-DD"T"HH24:MI:SS.###"Z"'
    , Postgres    : 'YYYY-MM-DD"T"HH24:MI:SS.######"Z"'
    , MySQL       : 'YYYY-MM-DD"T"HH24:MI:SS.######"Z"'
    , MariaDB     : 'YYYY-MM-DD"T"HH24:MI:SS.######"Z"'
    , MongoDB     : 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
    })
    return this._DATE_FORMAT_MASKS
  }

  static get TIMESTAMP_FORMAT_MASKS() { 
    this._TIMESTAMP_FORMAT_MASKS = this._TIMESTAMP_FORMAT_MASKS || Object.freeze({
      Oracle      : 'YYYY-MM-DD"T"HH24:MI:SS.FF9"Z"'
    , MSSQLSERVER : 'YYYY-MM-DD"T"HH24:MI:SS.FF7"Z"'
    , Postgres    : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"'
    , MySQL       : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"'
    , MariaDB     : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"'
    , MongoDB     : 'YYYY-MM-DD"T"HH24:MI:SS.FF9"Z"'
    , SNOWFLAKE   : 'YYYY-MM-DD"T"HH24:MI:SS.FF9"+00:00"' 
    })
    return this._TIMESTAMP_FORMAT_MASKS
  }

  static get DATA_TYPE_STRING_LENGTH() { 
    this._DATA_TYPE_STRING_LENGTH = this._DATA_TYPE_STRING_LENGTH || Object.freeze({
      BLOB          : this.LOB_STRING_MAX_LENGTH
    , CLOB          : this.LOB_STRING_MAX_LENGTH
    , JSON          : this.LOB_STRING_MAX_LENGTH
    , NCLOB         : this.LOB_STRING_MAX_LENGTH
    , OBJECT        : this.LOB_STRING_MAX_LENGTH
    , XMLTYPE       : this.LOB_STRING_MAX_LENGTH
    , ANYDATA       : this.LOB_STRING_MAX_LENGTH
    , BFILE         : this.BFILE_STRING_MAX_LENGTH
    , DATE          : 24
    , TIMESTAMP     : 30
    , INTERVAL      : 16
    })
    return this._DATA_TYPE_STRING_LENGTH
  }


  
  static get BIND_TYPES() {
    this._BIND_TYPES = this._BIND_TYPES || Object.freeze({
                                             [oracledb.BLOB]    : "BLOB"
                                           , [oracledb.BUFFER]  : "BUFFER"
                                           , [oracledb.CLOB]    : "CLOB"
                                           , [oracledb.CURSOR]  : "CURSOR"
                                           , [oracledb.DATE]    : "DATE"
                                           , [oracledb.DEFAULT] : "DEFAULT"
                                           , [oracledb.NCLOB]   : "NCLOB"
                                           , [oracledb.NUMBER]  : "NUMBER"
                                           , [oracledb.STRING]  : "STRING"
                                           })
    return this._BIND_TYPES
  }
  
  static NOT_CONNECTED()       { return 'DPI-1010:' }
  
  static INVALID_POOL()        { return 'NJS-002:' }
  
  static INVALID_CONNECTION()  { return 'NJS-003:' }
  
  static get MISSING_TABLE_ERROR() {
    this._MISSING_TABLE_ERROR = this._MISSING_TABLE_ERROR || Object.freeze([942])
    return this._MISSING_TABLE_ERROR
  }

  static get LOST_CONNECTION_ERROR() {
    this._LOST_CONNECTION_ERROR = this._LOST_CONNECTION_ERROR || Object.freeze([3113,3114,3135,28,1012])
    return this._LOST_CONNECTION_ERROR
  }

  static get SERVER_UNAVAILABLE_ERROR() {
    this._SERVER_UNAVAILABLE_ERROR = this._SERVER_UNAVAILABLE_ERROR || Object.freeze([1109,12514,12528,12537,12541])
    return this._SERVER_UNAVAILABLE_ERROR
  }

  static get SPATIAL_ERROR() {
    this._SPATIAL_ERROR = this._SPATIAL_ERROR || Object.freeze([29532])
    return this._SPATIAL_ERROR
  }

  static get JSON_PARSING_ERROR() {
    this._JSON_PARSING_ERROR = this._JSON_PARSING_ERROR || Object.freeze([40441])
    return this._JSON_PARSING_ERROR
  }

}

module.exports = OracleConstants// Driver defined constants

const _LOB_STRING_MAX_LENGTH    = 16 * 1024 * 1024;
// const _LOB_STRING_MAX_LENGTH    = 64 * 1024;
const _BFILE_STRING_MAX_LENGTH  =  2 * 1024;
const _STRING_MAX_LENGTH        =  4 * 1024;

