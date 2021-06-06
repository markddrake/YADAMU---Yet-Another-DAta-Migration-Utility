"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class RedshiftConstants {

  static get DATABASE_KEY()           { return 'redshift' };
  static get DATABASE_VENDOR()        { return 'Redshift' };
  static get SOFTWARE_VENDOR()        { return 'Amazon Web Services LLC'};

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "SPATIAL_FORMAT"            : "WKB"
    , "BYTE_TO_CHAR_RATIO"        : 4
    })
    return this._STATIC_PARAMETERS;
  }

  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#_DBI_PARAMETERS
  }

  static get COPY_OPERATION_SOURCES()    { return Object.freeze(['loader']) }

  static get FETCH_AS_STRING() { 
    this._FETCH_AS_STRING = this._FETCH_AS_STRING || Object.freeze([this.PGOID_DATE,this.PGOID_TIMESTAMP,this.PGOID_TIMESTAMP_TZ])
    return this._FETCH_AS_STRING;
  }

  static get SPATIAL_FORMAT()         { return this.DBI_PARAMETERS.SPATIAL_FORMAT };
  static get BYTE_TO_CHAR_RATIO()     { return this.DBI_PARAMETERS.BYTE_TO_CHAR_RATIO };
  static get STATEMENT_TERMINATOR()   { return ';' }

}

module.exports = RedshiftConstants