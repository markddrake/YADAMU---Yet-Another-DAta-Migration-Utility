"use strict"

import YadamuConstants from '../../lib/yadamuConstants.js';

class MongoConstants {

  static get DATABASE_KEY()           { return 'mongodb' };
  static get DATABASE_VENDOR()        { return 'MongoDB' };
  static get SOFTWARE_VENDOR()        { return 'Mongo Software Inc' };

  static get STATIC_PARAMETERS()      { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "PORT"                      : 27017
    , "MONGO_SAMPLE_LIMIT"        : 1000
    , "MONGO_STORAGE_FORMAT"      : "DOCUMENT"
    , "MONGO_EXPORT_FORMAT"       : "ARRAY"
    , "MONGO_STRIP_ID"            : false
    , "MONGO_PARSE_STRINGS"       : true
    , "DEFAULT_STRING_LENGTH"     : 32
	, "DOCUMENT_SIZE"             : 16777216
	, "CHAR_SIZE"                 : 16777216
	, "BINARY_SIZE"               : 16777216
	, "TIMESTAMP_PRECISION"       : 6
	, "NUMERIC_PRECISION"         : 53
    , "SPATIAL_FORMAT"            : "GeoJSON"
	, "DEFAULT_DATABASE"          : "admin"
    })
    return this._STATIC_PARAMETERS
  }

  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#_DBI_PARAMETERS
  }

  static get PORT()                   { return this.DBI_PARAMETERS.PORT}
  static get MONGO_SAMPLE_LIMIT()     { return this.DBI_PARAMETERS.MONGO_SAMPLE_LIMIT}
  static get MONGO_STORAGE_FORMAT()   { return this.DBI_PARAMETERS.MONGO_STORAGE_FORMAT}
  static get MONGO_EXPORT_FORMAT()    { return this.DBI_PARAMETERS.MONGO_EXPORT_FORMAT}
  static get MONGO_STRIP_ID()         { return this.DBI_PARAMETERS.MONGO_STRIP_ID}
  static get MONGO_PARSE_STRINGS()    { return this.DBI_PARAMETERS.MONGO_PARSE_STRINGS}
  static get DEFAULT_STRING_LENGTH()  { return this.DBI_PARAMETERS.DEFAULT_STRING_LENGTH}
  static get DOCUMENT_SIZE()          { return this.DBI_PARAMETERS.DOCUMENT_SIZE}
  static get CHAR_SIZE()              { return this.DBI_PARAMETERS.CHAR_SIZE}
  static get BINARY_SIZE()            { return this.DBI_PARAMETERS.BINARY_SIZE}
  static get TIMESTAMP_PRECISION()    { return this.DBI_PARAMETERS.TIMESTAMP_PRECISION}
  static get NUMERIC_PRECISION()      { return this.DBI_PARAMETERS.NUMERIC_PRECISION}
  static get DEFAULT_DATABASE()       { return this.DBI_PARAMETERS.DEFAULT_DATABASE };
  static get SPATIAL_FORMAT()         { return this.DBI_PARAMETERS.SPATIAL_FORMAT };

  static get STATEMENT_TERMINATOR()   { return ';' }

}

export { MongoConstants as default }