"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class OracleConstants {

  static get ORACLE_DEFAULTS() { 
    this._ORACLE_DEFAULTS = this._ORACLE_DEFAULTS || Object.freeze({
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
    return this._ORACLE_DEFAULTS;
  }

  static get DEFAULT_PARAMETERS() { 
    this._DEFAULT_PARAMETERS = this._DEFAULT_PARAMETERS || Object.freeze(Object.assign({},this.ORACLE_DEFAULTS,YadamuConstants.EXTERNAL_DEFAULTS.oracle))
    return this._DEFAULT_PARAMETERS
  }

  static get BATCH_LOB_COUNT()        { return this.DEFAULT_PARAMETERS.BATCH_LOB_COUNT}
  static get LOB_MIN_SIZE()           { return this.DEFAULT_PARAMETERS.LOB_MIN_SIZE}
  static get LOB_MAX_SIZE()           { return this.DEFAULT_PARAMETERS.LOB_MAX_SIZE}
  static get LOB_CACHE_COUNT()        { return this.DEFAULT_PARAMETERS.LOB_CACHE_COUNT}
  static get XML_STORAGE_FORMAT()     { return this.DEFAULT_PARAMETERS.XML_STORAGE_FORMAT}
  static get JSON_STORAGE_FORMAT()    { return this.DEFAULT_PARAMETERS.JSON_STORAGE_FORMAT}
  static get MIGRATE_JSON_STORAGE()   { return this.DEFAULT_PARAMETERS.MIGRATE_JSON_STORAGE}
  static get OBJECTS_AS_JSON()        { return this.DEFAULT_PARAMETERS.OBJECTS_AS_JSON}
  static get TREAT_RAW1_AS_BOOLEAN()  { return this.DEFAULT_PARAMETERS.TREAT_RAW1_AS_BOOLEAN}
  static get SPATIAL_FORMAT()         { return this.DEFAULT_PARAMETERS.SPATIAL_FORMAT };
  static get DATABASE_VENDOR()        { return 'Oracle' };
  static get SOFTWARE_VENDOR()        { return 'Oracle Corporation' };
  static get STATEMENT_TERMINATOR()   { return '/' }
 
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
  
}

module.exports = OracleConstants// Driver defined constants

const _LOB_STRING_MAX_LENGTH    = 16 * 1024 * 1024;
// const _LOB_STRING_MAX_LENGTH    = 64 * 1024;
const _BFILE_STRING_MAX_LENGTH  =  2 * 1024;
const _STRING_MAX_LENGTH        =  4 * 1024;

