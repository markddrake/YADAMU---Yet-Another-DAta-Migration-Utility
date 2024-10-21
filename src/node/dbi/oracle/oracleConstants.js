
import oracledb from 'oracledb';

import YadamuConstants from '../../lib/yadamuConstants.js';

class OracleConstants {

  static get DATABASE_KEY()           { return 'oracle' };
  static get DATABASE_VENDOR()        { return 'Oracle' };
  static get SOFTWARE_VENDOR()        { return 'Oracle Corporation' };
  static get STATEMENT_TERMINATOR()   { return '' }
  static get STATEMENT_SEPERATOR()    { return '\n/\n' }

  static get STATIC_DEFAULTS() {
    this._STATIC_DEFAULTS = this._STATIC_DEFAULTS || Object.freeze({
      "TEMPLOB_BATCH_LIMIT"        : 8192
    , "CACHELOB_BATCH_LIMIT"       : 65336							  
    , "CACHELOB_MAX_SIZE"          : 32767
    , "LOB_MAX_SIZE"               : 16777216
    , "VARCHAR_MAX_SIZE_EXTENDED"  : 32767
    , "VARCHAR_MAX_SIZE_STANDARD"  : 4000
    , "XML_STORAGE_OPTION"         : "XML"     // Optimize based on Database Version
    , "JSON_STORAGE_OPTION"        : "JSON"    // Optimize based on Database Version
    , "BOOLEAN_STORAGE_OPTION"     : "BOOLEAN"  // Recommended Default in lieu of native support - 0x1: True, 0x0: False
	, "OBJECT_STORAGE_OPTION"      : "NATIVE"  // Recommended Default, objects are stored as database objects
    , "MIGRATE_JSON_STORAGE"       : false
    , "PARTITION_LEVEL_OPERATIONS" : true
	, "BYTE_TO_CHAR_RATIO"         : 4
	, "COPY_LOGFILE_DIRNAME"       : null
	, "COPY_BADFILE_DIRNAME"       : null
    })
    return this._STATIC_DEFAULTS;
  }

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS() {
    this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_DEFAULTS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#DBI_PARAMETERS
  }

  static get TEMPLOB_BATCH_LIMIT()        { return this.DBI_PARAMETERS.TEMPLOB_BATCH_LIMIT}
  static get CACHELOB_BATCH_LIMIT()       { return this.DBI_PARAMETERS.CACHELOB_BATCH_LIMIT}
  static get CACHELOB_MAX_SIZE()          { return this.DBI_PARAMETERS.CACHELOB_MAX_SIZE}
																					
  static get LOB_MAX_SIZE()               { return this.DBI_PARAMETERS.LOB_MAX_SIZE}
  static get VARCHAR_MAX_SIZE_EXTENDED()  { return this.DBI_PARAMETERS.VARCHAR_MAX_SIZE_EXTENDED}
  static get VARCHAR_MAX_SIZE_STANDARD()  { return this.DBI_PARAMETERS.VARCHAR_MAX_SIZE_STANDARD}
  
  static get XML_STORAGE_OPTION()         { return this.DBI_PARAMETERS.XML_STORAGE_OPTION}
  static get JSON_STORAGE_OPTION()        { return this.DBI_PARAMETERS.JSON_STORAGE_OPTION}
  static get BOOLEAN_STORAGE_OPTION()     { return this.DBI_PARAMETERS.BOOLEAN_STORAGE_OPTION}
  static get OBJECT_STORAGE_OPTION()      { return this.DBI_PARAMETERS.OBJECT_STORAGE_OPTION}
  
  static get MIGRATE_JSON_STORAGE()       { return this.DBI_PARAMETERS.MIGRATE_JSON_STORAGE}
  
  
  static get PARTITION_LEVEL_OPERATIONS() { return this.DBI_PARAMETERS.PARTITION_LEVEL_OPERATIONS };
  static get BYTE_TO_CHAR_RATIO()         { return this.DBI_PARAMETERS.BYTE_TO_CHAR_RATIO };
  static get COPY_LOGFILE_DIRNAME()       { return this.DBI_PARAMETERS.COPY_LOGFILE_DIRNAME };
  static get COPY_BADFILE_DIRNAME()       { return this.DBI_PARAMETERS.COPY_BADFILE_DIRNAME };

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
    , Teradata    : 'YYYY-MM-DD"T"HH24:MI:SS.###"Z"'
	, DB2         : 'YYYY-MM-DD"T"HH24:MI:SS"Z"'
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
    , Teradata    : 'YYYY-MM-DD"T"HH24:MI:SS.FF6"Z"'
	, DB2         : 'YYYY-MM-DD"T"HH24:MI:SS.FF9"Z"'
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
                                                    [oracledb.DB_TYPE_BFILE         ]       : "DB_TYPE_BFILE"
                                                  , [oracledb.DB_TYPE_BINARY_DOUBLE ]       : "DB_TYPE_BINARY_DOUBLE"
                                                  , [oracledb.DB_TYPE_BINARY_FLOAT  ]       : "DB_TYPE_BINARY_FLOAT"
                                                  , [oracledb.DB_TYPE_BINARY_INTEGER]       : "DB_TYPE_BINARY_INTEGER"
                                                  , [oracledb.DB_TYPE_BLOB          ]       : "DB_TYPE_BLOB"
                                                  , [oracledb.DB_TYPE_BOOLEAN       ]       : "DB_TYPE_BOOLEAN"
                                                  , [oracledb.DB_TYPE_CHAR          ]       : "DB_TYPE_CHAR"
                                                  , [oracledb.DB_TYPE_CLOB          ]       : "DB_TYPE_CLOB"
                                                  , [oracledb.DB_TYPE_CURSOR        ]       : "DB_TYPE_CURSOR"
                                                  , [oracledb.DB_TYPE_DATE          ]       : "DB_TYPE_DATE"
                                                  , [oracledb.DB_TYPE_INTERVAL_DS   ]       : "DB_TYPE_INTERVAL_DS"
                                                  , [oracledb.DB_TYPE_INTERVAL_YM   ]       : "DB_TYPE_INTERVAL_YM"
                                                  , [oracledb.DB_TYPE_JSON          ]       : "DB_TYPE_JSON"
                                                  , [oracledb.DB_TYPE_LONG          ]       : "DB_TYPE_LONG"
                                                  , [oracledb.DB_TYPE_LONG_RAW      ]       : "DB_TYPE_LONG_RAW"
                                                  , [oracledb.DB_TYPE_NCHAR         ]       : "DB_TYPE_NCHAR"
                                                  , [oracledb.DB_TYPE_NCLOB         ]       : "DB_TYPE_NCLOB"
                                                  , [oracledb.DB_TYPE_NUMBER        ]       : "DB_TYPE_NUMBER"
                                                  , [oracledb.DB_TYPE_NVARCHAR      ]       : "DB_TYPE_NVARCHAR"
                                                  , [oracledb.DB_TYPE_OBJECT        ]       : "DB_TYPE_OBJECT"
                                                  , [oracledb.DB_TYPE_RAW           ]       : "DB_TYPE_RAW"
                                                  , [oracledb.DB_TYPE_ROWID         ]       : "DB_TYPE_ROWID"
                                                  , [oracledb.DB_TYPE_TIMESTAMP     ]       : "DB_TYPE_TIMESTAMP"
                                                  , [oracledb.DB_TYPE_TIMESTAMP_LTZ ]       : "DB_TYPE_TIMESTAMP_LTZ"
                                                  , [oracledb.DB_TYPE_TIMESTAMP_TZ  ]       : "DB_TYPE_TIMESTAMP_TZ"
                                                  , [oracledb.DB_TYPE_VARCHAR       ]       : "DB_TYPE_VARCHAR"
                                                  })
    return this._BIND_TYPES
  }


  static get DPI_NOT_CONNECTED()               { return 'DPI-1010' }

  static get DPI_CLOSED_CONNECTION()           { return 'DPI-1080' }

  static get NJS_INVALID_CONNECTION()          { return 'NJS-003' }

  static get NJS_INVALID_POOL()                { return 'NJS-002' }
  
  static get NJS_BROKEN_CONNECTION()           { return 'NJS-500' }

  static get KUP_FILE_NOT_FOUND()              { return 'KUP-0404' }

  static get MISSING_TABLE_ERROR() {
    this._MISSING_TABLE_ERROR = this._MISSING_TABLE_ERROR || Object.freeze([942])
    return this._MISSING_TABLE_ERROR
  }

  static get LOST_CONNECTION_ERROR() {
    this._LOST_CONNECTION_ERROR = this._LOST_CONNECTION_ERROR || Object.freeze([3113,3114,3135,28,1012])
    return this._LOST_CONNECTION_ERROR
  }

  static get ORACLEDB_LOST_CONNECTION() {
    this._ORACLEDB_LOST_CONNECTION = this._ORACLEDB_LOST_CONNECTION || Object.freeze([this.DPI_NOT_CONNECTED,this.DPI_CLOSED_CONNECTION,this.NJS_INVALID_CONNECTION,this.NJS_BROKEN_CONNECTION])
    return this._ORACLEDB_LOST_CONNECTION
  }


  static get SERVER_UNAVAILABLE_ERROR() {
    this._SERVER_UNAVAILABLE_ERROR = this._SERVER_UNAVAILABLE_ERROR || Object.freeze([1109,12514,12528,12537,12541])
    return this._SERVER_UNAVAILABLE_ERROR
  }

  static get SPATIAL_ERROR() {
    this._SPATIAL_ERROR = this._SPATIAL_ERROR || Object.freeze([13198,29532]) 
    return this._SPATIAL_ERROR
  }

  static get NONEXISTENT_USER() {
    this._NONEXISTENT_USER = this._NONEXISTENT_USER || Object.freeze([1918]) 
    return this._NONEXISTENT_USER
  }

  static get JSON_PARSING_ERROR() {
    this._JSON_PARSING_ERROR = this._JSON_PARSING_ERROR || Object.freeze([40441])
    return this._JSON_PARSING_ERROR
  }

  static get OCI_CALLOUT_ERROR() {
    this._OCI_CALLOUT_ERROR = this._OCI_CALLOUT_ERROR || Object.freeze([29913])
    return this._OCI_CALLOUT_ERROR
  }

  static get RECURSIVE_SQL_ERROR() {
    this._RECURSIVE_SQL_ERROR = this._RECURSIVE_SQL_ERROR || Object.freeze([604])
    return this._RECURSIVE_SQL_ERROR
  }

  static get LOCKING_ERROR() {
    this._LOCKING_ERROR = this._LOCKING_ERROR || Object.freeze([54])
    return this._LOCKING_ERROR
  }

}

export {OracleConstants as default }

// Driver defined constants

const _LOB_STRING_MAX_LENGTH    = 16 * 1024 * 1024;
// const _LOB_STRING_MAX_LENGTH    = 64 * 1024;
const _BFILE_STRING_MAX_LENGTH  =  2 * 1024;
const _STRING_MAX_LENGTH        =  4 * 1024;

