"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class MariadbConstants {

static get MARIADB_DEFAULTS() { 
    this._MARIADB_DEFAULTS = this._MARIADB_DEFAULTS || Object.freeze({
      "TABLE_MATCHING"            : "INSENSITIVE"
    , "TREAT_TINYINT1_AS_BOOLEAN" : true    
    , "SPATIAL_FORMAT"            : "WKB"
    })
    return this._MARIADB_DEFAULTS;
  }

  static get DEFAULT_PARAMETERS() { 
    this._DEFAULT_PARAMETERS = this._DEFAULT_PARAMETERS || Object.freeze(Object.assign({},this.MARIADB_DEFAULTS,YadamuConstants.EXTERNAL_DEFAULTS.mariadb))
    return this._DEFAULT_PARAMETERS
  }

  static get TABLE_MATCHING()             { return this.DEFAULT_PARAMETERS.TABLE_MATCHING}
  static get TREAT_TINYINT1_AS_BOOLEAN()  { return this.DEFAULT_PARAMETERS.TREAT_TINYINT1_AS_BOOLEAN}
  static get SPATIAL_FORMAT()             { return this.DEFAULT_PARAMETERS.SPATIAL_FORMAT };
  static get DATABASE_VENDOR()            { return 'MariaDB' };
  static get SOFTWARE_VENDOR()            { return 'MariaDB Corporation AB' };
  static get STATEMENT_TERMINATOR()       { return ';' }
 
  
  static get CONNECTION_PROPERTY_DEFAULTS() { 
    this._CONNECTION_PROPERTY_DEFAULTS = this._CONNECTION_PROPERTY_DEFAULTS || Object.freeze({
      multipleStatements: true
    , typeCast          : true
    , supportBigNumbers : true
    , bigNumberStrings  : true          
    , dateStrings       : true
    , rowsAsArray       : true
    , trace             : true
    })
   return this._CONNECTION_PROPERTY_DEFAULTS;
  }

}

module.exports = MariadbConstants