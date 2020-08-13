"use strict"

const YadamuDefaults = require('./yadamuDefaults.json');

class YadamuConstants {

  static get EXTERNAL_DEFAULTS() { return YadamuDefaults };

  static get YADAMU_DEFAULTS() {
    this._YADAMU_DEFAULTS = this._YADAMU_DEFAULTS || Object.freeze({
       "YADAMU_VERSION"            : '1.0'
     , "FILE"                      : "yadamu.json"
	 , "CONFIG"                    : "config.json"
     , "MODE"                      : "DATA_ONLY"
     , "ON_ERROR"                  : "ABORT"
     , "PARALLEL"                  : 0
     , "RDBMS"                     : "file"
     , "EXCEPTION_FOLDER"          : "exceptions"
     , "EXCEPTION_FILE_PREFIX"     : "exception"
     , "REJECTION_FOLDER"          : "rejections"
     , "REJECTION_FILE_PREFIX"     : "rejection"
     , "WARNING_FOLDER"            : "warnings"
     , "WARNING_FILE_PREFIX"       : "warning"
    })
    return this._YADAMU_DEFAULTS;
  }

  static get DEFAULT_PARAMETERS() { 
    this._DEFAULT_PARAMETERS = this._DEFAULT_PARAMETERS || Object.freeze(Object.assign({},this.YADAMU_DEFAULTS,this.EXTERNAL_DEFAULTS.yadamu))
    return this._DEFAULT_PARAMETERS
  }
   
  static get YADAMU_VERSION()         { return this.DEFAULT_PARAMETERS.YADAMU_VERSION }
  static get FILE()                   { return this.DEFAULT_PARAMETERS.FILE }
  static get CONFIG()                 { return this.DEFAULT_PARAMETERS.CONFIG }
  static get MODE()                   { return this.DEFAULT_PARAMETERS.MODE }
  static get ON_ERROR()               { return this.DEFAULT_PARAMETERS.ON_ERROR }
  static get PARALLEL()               { return this.DEFAULT_PARAMETERS.PARALLEL }
  static get RDBMS()                  { return this.DEFAULT_PARAMETERS.RDBMS }
  static get EXCEPTION_FOLDER()       { return this.DEFAULT_PARAMETERS.EXCEPTION_FOLDER }
  static get EXCEPTION_FILE_PREFIX()  { return this.DEFAULT_PARAMETERS.EXCEPTION_FILE_PREFIX }
  static get REJECTION_FOLDER()       { return this.DEFAULT_PARAMETERS.REJECTION_FOLDER }
  static get REJECTION_FILE_PREFIX()  { return this.DEFAULT_PARAMETERS.REJECTION_FILE_PREFIX }
  static get WARNING_FOLDER()         { return this.DEFAULT_PARAMETERS.WARNING_FOLDER }
  static get WARNING_FILE_PREFIX()    { return this.DEFAULT_PARAMETERS.WARNING_FILE_PREFIX }
  
  static get YADAMU_DRIVERS()         { return this.EXTERNAL_DEFAULTS.drivers }

}

module.exports = YadamuConstants