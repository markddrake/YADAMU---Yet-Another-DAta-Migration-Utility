
import YadamuConstants from '../../lib/yadamuConstants.js';

class VerticaConstants {

  static get DATABASE_KEY()           { return 'vertica' };
  static get DATABASE_VENDOR()        { return 'Vertica' };
  static get SOFTWARE_VENDOR()        { return 'Micro Focus International plc' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
	  "COPY_TRIM_WHITEPSPACE"     : false
    , "MERGEOUT_INSERT_COUNT"     : 128                                                             
	, "BYTE_TO_CHAR_RATIO"        : 4
	, "XML_STORAGE_OPTION"        : "long varchar"
	, "JSON_STORAGE_OPTION"       : "long varchar"
    })
    return this._STATIC_PARAMETERS;
  }

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#DBI_PARAMETERS
  }

  static get COPY_TRIM_WHITEPSPACE()  { return this.DBI_PARAMETERS.COPY_TRIM_WHITEPSPACE === true }
  static get BYTE_TO_CHAR_RATIO()     { return this.DBI_PARAMETERS.BYTE_TO_CHAR_RATIO }
  static get MERGEOUT_INSERT_COUNT()  { return this.DBI_PARAMETERS.MERGEOUT_INSERT_COUNT }
  static get XML_STORAGE_OPTION()     { return this.DBI_PARAMETERS.XML_STORAGE_OPTION }
  static get JSON_STORAGE_OPTION()    { return this.DBI_PARAMETERS.JSON_STORAGE_OPTION }
    
  static get STATEMENT_TERMINATOR()   { return ';' }

}

export { VerticaConstants as default }

const PGOID_DATE         = 1082; 
const PGOID_TIMESTAMP    = 1114;
const PGOID_TIMESTAMP_TZ = 1118;

