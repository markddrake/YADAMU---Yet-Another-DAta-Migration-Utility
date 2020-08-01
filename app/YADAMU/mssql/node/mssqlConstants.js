"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class MsSQLConstants {
    
  static get MsSQL_DEFAULTS() { 
    this._MsSQL_DEFAULTS = this._MsSQL_DEFAULTS || Object.freeze({
      "DEFAULT_USER"              : "dbo"
    , "SPATIAL_MAKE_VALID"        : false
    , "SPATIAL_FORMAT"            : "WKB"
    })
    return this._MsSQL_DEFAULTS;
  }

  static get DEFAULT_PARAMETERS() { 
    this._DEFAULT_PARAMETERS = this._DEFAULT_PARAMETERS || Object.freeze(Object.assign({},this.MsSQL_DEFAULTS,YadamuConstants.EXTERNAL_DEFAULTS.mssql))
    return this._DEFAULT_PARAMETERS
  }

  static get DEFAULT_USER()           { return this.DEFAULT_PARAMETERS.DEFAULT_USER}
  static get SPATIAL_FORMAT()         { return this.DEFAULT_PARAMETERS.SPATIAL_FORMAT };
  static get SPATIAL_MAKE_VALID()     { return this.DEFAULT_PARAMETERS.SPATIAL_MAKE_VALID };
  static get DATABASE_VENDOR()        { return 'MSSQLSERVER' };
  static get SOFTWARE_VENDOR()        { return 'Microsoft Corporation' };
  static get STATEMENT_TERMINATOR()   { return 'go' }
 
  static get STAGING_TABLE () { 
    this._STAGING_TABLE = this._STAGING_TABLE || Object.freeze({
      tableName  : '#YADAMU_STAGING'
    , columnName : 'DATA'    
    })
    return this._STAGING_TABLE
  }

}

module.exports = MsSQLConstants