"use strict"

const YadamuConstants = require('../../common/yadamuConstants.js');

class ExampleConstants {

  static get EXAMPLE_DEFAULTS() { 
    this._EXAMPLE_DEFAULTS = this._EXAMPLE_DEFAULTS || Object.freeze({
      "SPATIAL_FORMAT"            : "WKB"
    })
    return this._EXAMPLE_DEFAULTS;
  }

  static get DEFAULT_PARAMETERS() { 
    this._DEFAULT_PARAMETERS = this._DEFAULT_PARAMETERS || Object.freeze(Object.assign({},this.EXAMPLE_DEFAULTS,YadamuConstants.YADAMU_DEFAULTS.example || {}))
    return this._DEFAULT_PARAMETERS
  }
  
  static get SPATIAL_FORMAT()             { return this.DEFAULT_PARAMETERS.SPATIAL_FORMAT };
  static get DATABASE_VENDOR()            { return 'Vendor' };
  static get SOFTWARE_VENDOR()            { return 'Vendor Corporation' };
  static get STATEMENT_TERMINATOR()       { return ';' }

}

module.exports = ExampleConstants