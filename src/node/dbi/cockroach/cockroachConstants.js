
import YadamuConstants from '../../lib/yadamuConstants.js';

class PostgresConstants {

  static get DATABASE_KEY()           { return 'cockroach' };
  static get DATABASE_VENDOR()        { return 'Cockroach' };
  static get SOFTWARE_VENDOR()        { return 'Cockroach Labs' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
	  "COCKROACH_STRIP_ROWID"        : false
	, "BYTEA_SIZING_MODEL"           : "100%"
	, "TIMESTAMP_PRECISION"          : 6
	, "COPY_SERVER_NAME"             : "YADAMU_CSV_SERVER"
	, "MAX_READ_BUFFER_MESSAGE_SIZE" : "50 MiB"
    })
    return this._STATIC_PARAMETERS;
  }

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#DBI_PARAMETERS
  }

  static get PGOID_DATE()          { return _PGOID_DATE }
  static get PGOID_TIMESTAMP()     { return _PGOID_TIMESTAMP }
  static get PGOID_TIMESTAMP_TZ()  { return _PGOID_TIMESTAMP_TZ }

  static get FETCH_AS_STRING() { 
    this._FETCH_AS_STRING = this._FETCH_AS_STRING || Object.freeze([this.PGOID_DATE,this.PGOID_TIMESTAMP,this.PGOID_TIMESTAMP_TZ])
    return this._FETCH_AS_STRING;
  }

  static get BYTEA_SIZING_MODEL()           { return this.DBI_PARAMETERS.BYTEA_SIZING_MODEL };
  static get COPY_SERVER_NAME()             { return this.DBI_PARAMETERS.COPY_SERVER_NAME };
  static get COCKROACH_STRIP_ROWID()        { return this.DBI_PARAMETERS.COCKROACH_STRIP_ROWID };
  static get MAX_READ_BUFFER_MESSAGE_SIZE() { return this.DBI_PARAMETERS.MAX_READ_BUFFER_MESSAGE_SIZE };
  static get STATEMENT_TERMINATOR()         { return ';' }

}

export { PostgresConstants as default }

const PGOID_DATE         = 1082; 
const PGOID_TIMESTAMP    = 1114;
const PGOID_TIMESTAMP_TZ = 1118;