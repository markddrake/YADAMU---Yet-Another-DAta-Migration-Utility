"use strict"

const oracledb = require('oracledb');

const YadamuConstants = require('../../common/yadamuConstants.js');

class OracleConstants {

  static get DATABASE_KEY()           { return 'oracle' };
  static get DATABASE_VENDOR()        { return 'Oracle' };
  static get SOFTWARE_VENDOR()        { return 'Oracle Corporation' };
  static get STATEMENT_TERMINATOR()   { return '' }
  static get STATEMENT_SEPERATOR()    { return '\n/\n' }

  static get STATIC_DEFAULTS() { 
    this._STATIC_DEFAULTS = this._STATIC_DEFAULTS || Object.freeze({
      "BATCH_TEMPLOB_LIMIT"       : 8192
    , "BATCH_CACHELOB_LIMIT"      : 65336
    , "ORACLE_XML_TYPE"           : "XML"
    , "LOB_MIN_SIZE"              : 32768
    , "LOB_MAX_SIZE"              : 16777216
    , "ORACLE_JSON_TYPE"          : "JSON"
    , "MIGRATE_JSON_STORAGE"      : false
    , "OBJECT_FORMAT"             : "NATIVE"
    , "TREAT_RAW1_AS_BOOLEAN"     : true
    , "SPATIAL_FORMAT"            : "WKB"
	, "BYTE_TO_CHAR_RATIO"        : 4
	, "COPY_LOGFILE_DIRNAME"      : null
	, "COPY_BADFILE_DIRNAME"      : null
    })
    return this._STATIC_DEFAULTS;
  }

  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_DEFAULTS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#_DBI_PARAMETERS
  }

  static get BATCH_TEMPLOB_LIMIT()    { return this.DBI_PARAMETERS.BATCH_TEMPLOB_LIMIT}
  static get BATCH_CACHELOB_LIMIT()   { return this.DBI_PARAMETERS.BATCH_CACHELOB_LIMIT}
  static get LOB_MIN_SIZE()           { return this.DBI_PARAMETERS.LOB_MIN_SIZE}
  static get LOB_MAX_SIZE()           { return this.DBI_PARAMETERS.LOB_MAX_SIZE}
  static get ORACLE_XML_TYPE()        { return this.DBI_PARAMETERS.ORACLE_XML_TYPE}
  static get ORACLE_JSON_TYPE()       { return this.DBI_PARAMETERS.ORACLE_JSON_TYPE}
  static get MIGRATE_JSON_STORAGE()   { return this.DBI_PARAMETERS.MIGRATE_JSON_STORAGE}
  static get OBJECT_FORMAT()          { return this.DBI_PARAMETERS.OBJECT_FORMAT}
  static get TREAT_RAW1_AS_BOOLEAN()  { return this.DBI_PARAMETERS.TREAT_RAW1_AS_BOOLEAN}
  static get SPATIAL_FORMAT()         { return this.DBI_PARAMETERS.SPATIAL_FORMAT };
  static get BYTE_TO_CHAR_RATIO()     { return this.DBI_PARAMETERS.BYTE_TO_CHAR_RATIO };
  static get COPY_LOGFILE_DIRNAME()   { return this.DBI_PARAMETERS.COPY_LOGFILE_DIRNAME };
  static get COPY_BADFILE_DIRNAME()   { return this.DBI_PARAMETERS.COPY_BADFILE_DIRNAME };

  // Until we have static constants

  static get LOB_STRING_MAX_LENGTH()   { return _LOB_STRING_MAX_LENGTH }
  static get BFILE_STRING_MAX_LENGTH() { return _BFILE_STRING_MAX_LENGTH }
  static get STRING_MAX_LENGTH()       { return _STRING_MAX_LENGTH }

  static get DATE_FORMAT_MASKS() { 
    this._DATE_FORMAT_MASKS = this._DATE_FORMAT_MASKS || Object.freeze({
      Oracle      : 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
    , MSSQLSERVER : 'YYYY-MM-DD"T"HH24:MI:SS.###"Z"'
    , Postgres    : 'YYYY-MM-DD"T"HH24:MI:SS.######"Z"'
    , Vertica     : 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
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
    , Vertica     : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"+00:00"'
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

  static get COPY_FILE_NOT_FOUND_ERROR() {
    this._COPY_FILE_NOT_FOUND_ERROR = this._COPY_FILE_NOT_FOUND_ERROR || Object.freeze([29913])
    return this._COPY_FILE_NOT_FOUND_ERROR
  }

  static get STAGED_DATA_SOURCES()    { return Object.freeze(['loader']) }

}

module.exports = OracleConstants// Driver defined constants

const _LOB_STRING_MAX_LENGTH    = 16 * 1024 * 1024;
// const _LOB_STRING_MAX_LENGTH    = 64 * 1024;
const _BFILE_STRING_MAX_LENGTH  =  2 * 1024;
const _STRING_MAX_LENGTH        =  4 * 1024;

