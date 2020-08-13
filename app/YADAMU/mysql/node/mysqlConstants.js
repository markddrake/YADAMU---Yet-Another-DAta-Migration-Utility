"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class MySQLConstants {

  static get MYSQL_DEFAULTS() { 
    this._MYSQL_DEFAULTS = this._MYSQL_DEFAULTS || Object.freeze({
      "TABLE_MATCHING"            : "INSENSITIVE"
    , "READ_KEEP_ALIVE"           : 0
    , "TREAT_TINYINT1_AS_BOOLEAN" : true    
    , "SPATIAL_FORMAT"            : "WKB"
    })
    return this._MYSQL_DEFAULTS;
  }

  static get DEFAULT_PARAMETERS() { 
    this._DEFAULT_PARAMETERS = this._DEFAULT_PARAMETERS || Object.freeze(Object.assign({},this.MYSQL_DEFAULTS,YadamuConstants.EXTERNAL_DEFAULTS.mysql || {}))
    return this._DEFAULT_PARAMETERS
  }

  static get TABLE_MATCHING()             { return this.DEFAULT_PARAMETERS.TABLE_MATCHING}
  static get READ_KEEP_ALIVE()            { return this.DEFAULT_PARAMETERS.READ_KEEP_ALIVE}
  static get TREAT_TINYINT1_AS_BOOLEAN()  { return this.DEFAULT_PARAMETERS.TREAT_TINYINT1_AS_BOOLEAN}
  static get SPATIAL_FORMAT()             { return this.DEFAULT_PARAMETERS.SPATIAL_FORMAT };
  static get DATABASE_VENDOR()            { return 'MySQL' };
  static get SOFTWARE_VENDOR()            { return 'Oracle Corporation (MySQL)' };
  static get STATEMENT_TERMINATOR()       { return ';' }
 
  static get CONNECTION_PROPERTY_DEFAULTS() { 
    this._CONNECTION_PROPERTY_DEFAULTS = this._CONNECTION_PROPERTY_DEFAULTS || Object.freeze({
      multipleStatements: true
    , typeCast          : true
    , supportBigNumbers : true
    , bigNumberStrings  : true          
    , dateStrings       : true
    , trace             : true
    })
   return this._CONNECTION_PROPERTY_DEFAULTS;
  }

}

module.exports = MySQLConstants