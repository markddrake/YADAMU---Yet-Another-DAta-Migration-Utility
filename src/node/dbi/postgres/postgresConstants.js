"use strict"

import YadamuConstants from '../../common/yadamuConstants.js';

class PostgresConstants {

  static get DATABASE_KEY()           { return 'postgres' };
  static get DATABASE_VENDOR()        { return 'Postgres' };
  static get SOFTWARE_VENDOR()        { return 'The PostgreSQL Global Development Group' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "SPATIAL_FORMAT"            : "WKB"
	, "CIRCLE_FORMAT"             : "POLYGON"  /* Portable as spatial type by leads to loss of fidelity  */
	, "POSTGRES_JSON_TYPE"        : "JSONB"
	, "BYTEA_SIZING_MODEL"        : "100%"
	, "TIMESTAMP_PRECISION"       : 6
	, "COPY_SERVER_NAME"          : "YADAMU_CSV_SERVER"
    })
    return this._STATIC_PARAMETERS;
  }

  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#_DBI_PARAMETERS
  }

  static get PGOID_DATE()          { return _PGOID_DATE }
  static get PGOID_TIMESTAMP()     { return _PGOID_TIMESTAMP }
  static get PGOID_TIMESTAMP_TZ()  { return _PGOID_TIMESTAMP_TZ }

  static get FETCH_AS_STRING() { 
    this._FETCH_AS_STRING = this._FETCH_AS_STRING || Object.freeze([this.PGOID_DATE,this.PGOID_TIMESTAMP,this.PGOID_TIMESTAMP_TZ])
    return this._FETCH_AS_STRING;
  }

  static get SPATIAL_FORMAT()         { return this.DBI_PARAMETERS.SPATIAL_FORMAT };
  static get CIRCLE_FORMAT()          { return this.DBI_PARAMETERS.CIRCLE_FORMAT };
  static get BYTEA_SIZING_MODEL()     { return this.DBI_PARAMETERS.BYTEA_SIZING_MODEL };
  static get POSTGRES_JSON_TYPE()     { return this.DBI_PARAMETERS.POSTGRES_JSON_TYPE };
  static get COPY_SERVER_NAME()       { return this.DBI_PARAMETERS.COPY_SERVER_NAME };
  static get STATEMENT_TERMINATOR()   { return ';' }

  static get STAGED_DATA_SOURCES()    { return Object.freeze(['loader']) }

}

export { PostgresConstants as default }