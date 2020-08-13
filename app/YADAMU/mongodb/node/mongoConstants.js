"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class MongoConstants {

  static get MONGO_DEFAULTS()      { 
    this._MONGO_DEFAULTS = this._MONGO_DEFAULTS || Object.freeze({
      "PORT"                      : 27017
    , "MONGO_SAMPLE_LIMIT"        : 1000
    , "MONGO_STORAGE_FORMAT"      : "DOCUMENT"
    , "MONGO_EXPORT_FORMAT"       : "ARRAY"
    , "MONGO_STRIP_ID"            : false
    , "DEFAULT_STRING_LENGTH"     : "32"
    , "MAX_STRING_LENGTH"         : "16777216"
    , "SPATIAL_FORMAT"            : "GeoJSON"
    })
    return this._MONGO_DEFAULTS
  }

  static get DEFAULT_PARAMETERS() { 
    this._DEFAULT_PARAMETERS = this._DEFAULT_PARAMETERS || Object.freeze(Object.assign({},this.MONGO_DEFAULTS,YadamuConstants.EXTERNAL_DEFAULTS.mongo || {}))
    return this._DEFAULT_PARAMETERS
  }

  static get PORT()                   { return this.DEFAULT_PARAMETERS.PORT}
  static get MONGO_SAMPLE_LIMIT()     { return this.DEFAULT_PARAMETERS.MONGO_SAMPLE_LIMIT}
  static get MONGO_STORAGE_FORMAT()   { return this.DEFAULT_PARAMETERS.MONGO_STORAGE_FORMAT}
  static get MONGO_EXPORT_FORMAT()    { return this.DEFAULT_PARAMETERS.MONGO_EXPORT_FORMAT}
  static get MONGO_STRIP_ID()         { return this.DEFAULT_PARAMETERS.MONGO_STRIP_ID}
  static get DEFAULT_STRING_LENGTH()  { return this.DEFAULT_PARAMETERS.DEFAULT_STRING_LENGTH}
  static get MAX_STRING_LENGTH()      { return this.DEFAULT_PARAMETERS.MAX_STRING_LENGTH}
  static get SPATIAL_FORMAT()         { return this.DEFAULT_PARAMETERS.SPATIAL_FORMAT };
  static get DATABASE_VENDOR()        { return 'MongoDB' };
  static get SOFTWARE_VENDOR()        { return 'Mongo Software Inc' };
  static get STATEMENT_TERMINATOR()   { return ';' }

}

module.exports = MongoConstants