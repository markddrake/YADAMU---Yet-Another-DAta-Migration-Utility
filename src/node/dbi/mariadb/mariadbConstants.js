
import YadamuConstants from '../../lib/yadamuConstants.js';

class MariadbConstants {

  static get DATABASE_KEY()               { return 'mariadb' };
  static get SOFTWARE_VENDOR()            { return 'MariaDB Corporation AB' };
  static get DATABASE_VENDOR()            { return 'MariaDB' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
	  "BOOLEAN_STORAGE_OPTION"    : "tinyint(1)"
	, "SET_STORAGE_OPTION"        : "json"
	, "ENUM_STORAGE_OPTION"       : "varchar(512)"		
	, "XML_STORAGE_OPTION"        : "longtext"
    })
    return this._STATIC_PARAMETERS;
  }
  
  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#_DBI_PARAMETERS
  }

  static get BOOLEAN_STORAGE_OPTION()     { return this.DBI_PARAMETERS.BOOLEAN_STORAGE_OPTION}
  static get SET_STORAGE_OPTION()         { return this.SET_STORAGE_OPTION}
  static get ENUM_STORAGE_OPTION()        { return this.DBI_PARAMETERS.ENUM_STORAGE_OPTION}
  static get XML_STORAGE_OPTION()         { return this.DBI_PARAMETERS.XML_STORAGE_OPTION}

  static get STATEMENT_TERMINATOR()       { return ';' }
 
  
  static get CONNECTION_PROPERTY_DEFAULTS() { 
    this._CONNECTION_PROPERTY_DEFAULTS = this._CONNECTION_PROPERTY_DEFAULTS || Object.freeze({
      multipleStatements: true
    , typeCast          : true
    , bigNumberStrings  : true          
    , dateStrings       : true
    , rowsAsArray       : true
    , trace             : true
    })
   return this._CONNECTION_PROPERTY_DEFAULTS;
  }

}

export { MariadbConstants as default }