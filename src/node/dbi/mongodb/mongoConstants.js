
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
    , "DEFAULT_STRING_LENGTH"     : "32"
    , "MAX_STRING_LENGTH"         : "16777216"
	, "MAX_DOCUMENT_SIZE"         : "16777216"
	, "TIMESTAMP_PRECISION"       : 6
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
  static get MAX_STRING_LENGTH()      { return this.DBI_PARAMETERS.MAX_STRING_LENGTH}
  static get STATEMENT_TERMINATOR()   { return ';' }
  static get MAX_DOCUMENT_SIZE()      { return this.DBI_PARAMETERS.MAX_DOCUMENT_SIZE}
  static get DEFAULT_DATABASE()       { return this.DBI_PARAMETERS.DEFAULT_DATABASE };

  static get SESSION_ENDED_ERROR() {
    this._SESSION_ENDED_ERROR = this._SESSION_ENDED_ERROR || Object.freeze([11600])
    return this._SESSION_ENDED_ERROR
  }

  static get SESSION_ENDED_MESSAGE() {
    this._SESSION_ENDED_MESSAGE = this._SESSION_ENDED_MESSAGE || Object.freeze(["Cannot use a session that has ended"])
    return this._SESSION_ENDED_MESSAGE
  }
  
  static get SERVER_UNAVAILABLE_ERROR() {
    this._SERVER_UNAVAILABLE_ERROR = this._SERVER_UNAVAILABLE_ERROR || Object.freeze([11600])
    return this._SERVER_UNAVAILABLE_ERROR
  }

  static get SERVER_UNAVAILABLE_MESSAGE() {
    this._SERVER_UNAVAILABLE_MESSAGE = this._SERVER_UNAVAILABLE_MESSAGE || Object.freeze(["pool is draining, new operations prohibited"])
    return this._SERVER_UNAVAILABLE_MESSAGE
  }

  static get CONTENT_TOO_LARGE_ERROR() {
    this._CONTENT_TOO_LARGE_ERROR = this._CONTENT_TOO_LARGE_ERROR || Object.freeze(['ERR_OUT_OF_RANGE'])
    return this._CONTENT_TOO_LARGE_ERROR
  }

  static get CONTENT_TOO_LARGE_MESSAGE() {
    this._CONTENT_TOO_LARGE_MESSAGE = this._CONTENT_TOO_LARGE_MESSAGE || Object.freeze(["document is larger than the maximum size 16777216"])
    return this._CONTENT_TOO_LARGE_MESSAGE
  }

}

export { MongoConstants as default }