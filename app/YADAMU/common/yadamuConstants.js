"use strict"

const YadamuDefaults = require('./yadamuDefaults.json');

class YadamuConstants {

  static get EXTERNAL_DEFAULTS() { return YadamuDefaults };

  static get YADAMU_DEFAULTS() {
    this._YADAMU_DEFAULTS = this._YADAMU_DEFAULTS || Object.freeze({
       "YADAMU_VERSION"            : '1.0'
     , "FILE"                      : "yadamu.json"
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
   
  static get YADAMU_VERSION()         { return this.YADAMU_DEFAULTS.YADAMU_VERSION }
  static get FILE()                   { return this.YADAMU_DEFAULTS.FILE }
  static get MODE()                   { return this.YADAMU_DEFAULTS.MODE }
  static get ON_ERROR()               { return this.YADAMU_DEFAULTS.ON_ERROR }
  static get PARALLEL()               { return this.YADAMU_DEFAULTS.PARALLEL }
  static get RDBMS()                  { return this.YADAMU_DEFAULTS.RDBMS }
  static get EXCEPTION_FOLDER()       { return this.YADAMU_DEFAULTS.EXCEPTION_FOLDER }
  static get EXCEPTION_FILE_PREFIX()  { return this.YADAMU_DEFAULTS.EXCEPTION_FILE_PREFIX }
  static get REJECTION_FOLDER()       { return this.YADAMU_DEFAULTS.REJECTION_FOLDER }
  static get REJECTION_FILE_PREFIX()  { return this.YADAMU_DEFAULTS.REJECTION_FILE_PREFIX }
  static get WARNING_FOLDER()         { return this.YADAMU_DEFAULTS.WARNING_FOLDER }
  static get WARNING_FILE_PREFIX()    { return this.YADAMU_DEFAULTS.WARNING_FILE_PREFIX }
  
  static get YADAMU_DRIVERS()         { return this.EXTERNAL_DEFAULTS.drivers }

}

module.exports = YadamuConstants