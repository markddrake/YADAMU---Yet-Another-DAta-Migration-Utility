"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class PostgresConstants {

  static get POSTGRES_DEFAULTS() { 
    this._POSTGRES_DEFAULTS = this._POSTGRES_DEFAULTS || Object.freeze({
      "SPATIAL_FORMAT"            : "WKB"
    })
    return this._POSTGRES_DEFAULTS;
  }

  static get DEFAULT_PARAMETERS() { 
    this._DEFAULT_PARAMETERS = this._DEFAULT_PARAMETERS || Object.freeze(Object.assign({},this.POSTGRES_DEFAULTS,YadamuConstants.EXTERNAL_DEFAULTS.postgres))
    return this._DEFAULT_PARAMETERS
  }

  static get PGOID_DATE()          { return _PGOID_DATE }
  static get PGOID_TIMESTAMP()     { return _PGOID_TIMESTAMP }
  static get PGOID_TIMESTAMP_TZ()  { return _PGOID_TIMESTAMP_TZ }

  static get FETCH_AS_STRING() { 
    this._FETCH_AS_STRING = this._FETCH_AS_STRING || Object.freeze([this.PGOID_DATE,this.PGOID_TIMESTAMP,this.PGOID_TIMESTAMP_TZ])
    return this._FETCH_AS_STRING;
  }

  static get SPATIAL_FORMAT()         { return OracleDBI.DEFAULT_PARAMETERS.SPATIAL_FORMAT };
  static get DATABASE_VENDOR()        { return 'Postgres' };
  static get SOFTWARE_VENDOR()        { return 'The PostgreSQL Global Development Group' };
  static get STATEMENT_TERMINATOR()   { return '/' }

}

module.exports = PostgresConstants