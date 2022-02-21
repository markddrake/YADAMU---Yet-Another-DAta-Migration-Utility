"use strict"

import crypto from 'crypto';

import fs from 'fs'
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

// const YadamuDefaults = require('./yadamuDefaults.json');

const  __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const YadamuDefaults = JSON.parse(fs.readFileSync(join(__dirname,'./yadamuDefaults.json'),'utf-8'));
  
class YadamuConstants {

  static get DESTROYED()              { return 'destroyed' }
  static get DDL_COMPLETE()           { return 'ddlComplete' }
  static get DDL_UNNECESSARY()        { return 'ddlUnnecessary' }
  static get CACHE_LOADED()           { return 'cacheLoaded'  }
  static get DB_CONNECTED()           { return 'dbConnected'}
  static get END_OF_DATA()            { return 'eod'}
  static get END_OF_FILE()            { return 'eof'}
  
  static get READER_ROLE()            { return 'READER' }
  static get WRITER_ROLE()            { return 'WRITER' }

  static get YADAMU_CONFIGURATION() { return YadamuDefaults };

  static get STATIC_PARAMETERS() {
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
       "YADAMU_VERSION"            : '2.0'
     , "FILE"                      : "yadamu.json"
	 , "CONFIG"                    : "config.json"
     , "RDBMS"                     : "file"
     , "EXCEPTION_FOLDER"          : "exceptions"
     , "EXCEPTION_FILE_PREFIX"     : "exception"
     , "REJECTION_FOLDER"          : "rejections"
     , "REJECTION_FILE_PREFIX"     : "rejection"
     , "WARNING_FOLDER"            : "warnings"
     , "WARNING_FILE_PREFIX"       : "warning"
	 , "IDENTIFIER_TRANSFORMATION" : 'NONE'
	 , "CIPHER"                    : 'aes-256-cbc'
     , "ENCRYPTION"                : true
	 , "SALT"                      : "YABASCYADAMUUMADAYCSABAY"
    })
    return this._STATIC_PARAMETERS;
  }

  static get YADAMU_PARAMETERS() { 
    this._YADAMU_PARAMETERS = this._YADAMU_PARAMETERS || Object.freeze(Object.assign({},this.STATIC_PARAMETERS,this.YADAMU_CONFIGURATION.yadamu))
	return this._YADAMU_PARAMETERS
  }
  
  static get ABORT_CURRENT_TABLE() {
	this._ABORT_CURRENT_TABLE  = this._ABORT_CURRENT_TABLE || Object.freeze(['ABORT','SKIP'])
	return this._ABORT_CURRENT_TABLE
  }
  
  static get ABORT_PROCESSING() {
	this._ABORT_PROCESSING  = this._ABORT_PROCESSING || Object.freeze(['ABORT',undefined])
	return this._ABORT_PROCESSING
  }

  static get CONTINUE_PROCESSING() {
	this._CONTINUE_PROCESSING = this._CONTINUE_PROCESSING || Object.freeze(['SKIP','FLUSH'])
	return this._CONTINUE_PROCESSING
  }
  
  static get PRODUCT_SHORT_NAME()              { return 'YADAMU' }
  static get PRODUCT_NAME()                    { return 'Yet Another DAta Migration Utility' }
  static get COMPANY_SHORT_NAME()              { return 'YABASC' }
  static get COMPANY_NAME()                    { return 'Yet Another Bay Area Software Company' }
                                              
  static get YADAMU_VERSION()                  { return this.YADAMU_PARAMETERS.YADAMU_VERSION }
  static get FILE()                            { return this.YADAMU_PARAMETERS.FILE }
  static get CONFIG()                          { return this.YADAMU_PARAMETERS.CONFIG }
  static get RDBMS()                           { return this.YADAMU_PARAMETERS.RDBMS }
  static get EXCEPTION_FOLDER()                { return this.YADAMU_PARAMETERS.EXCEPTION_FOLDER }
  static get EXCEPTION_FILE_PREFIX()           { return this.YADAMU_PARAMETERS.EXCEPTION_FILE_PREFIX }
  static get REJECTION_FOLDER()                { return this.YADAMU_PARAMETERS.REJECTION_FOLDER }
  static get REJECTION_FILE_PREFIX()           { return this.YADAMU_PARAMETERS.REJECTION_FILE_PREFIX }
  static get WARNING_FOLDER()                  { return this.YADAMU_PARAMETERS.WARNING_FOLDER }
  static get WARNING_FILE_PREFIX()             { return this.YADAMU_PARAMETERS.WARNING_FILE_PREFIX }
  static get IDENTIFIER_TRANSFORMATION()       { return this.YADAMU_PARAMETERS.IDENTIFIER_TRANSFORMATION }


  static get ENCRYPTION_ALGORITM()             { return this.YADAMU_PARAMETERS.ENCRYPTION_ALGORITM }
  static get ENCRYPTION()                      { return this.YADAMU_PARAMETERS.ENCRYPTION }
  static get SALT()                            { return this.YADAMU_PARAMETERS.SALT }
  
  static get YADAMU_DRIVERS()                  { return this.YADAMU_CONFIGURATION.drivers }
  
  static get SAVE_POINT_NAME()                 { return 'YADAMU_INSERT' }

  static get TEXTUAL_MIME_TYPES() { 
    this._TEXTUAL_MIME_TYPES = this._TEXTUAL_MIME_TYPES || Object.freeze(["application/json","application/csv"])
    return this._TEXTUAL_MIME_TYPES
  }

  static get SUPPORTED_IDENTIFIER_TRANSFORMATION() {
    this._SUPPORTED_IDENTIFIER_TRANSFORMATION = this._SUPPORTED_COMPRESSION || Object.freeze(["NONE","UPPERACSE","LOWERCASE"])
	return this._SUPPORTED_IDENTIFIER_TRANSFORMATION
  }
    
  static get SUPPORTED_COMPRESSION() {
    this._SUPPORTED_COMPRESSION = this._SUPPORTED_COMPRESSION || Object.freeze(["GZIP","INFLATE"])
	return this._SUPPORTED_COMPRESSION
  }
  
  static get TRUE_OR_FALSE() {
    this._TRUE_OR_FALSE = this._TRUE_OR_FALSE || Object.freeze(["TRUE","FALSE"])
	return this._TRUE_OR_FALSE
  }
  
  static get SUPPORTED_CIPHER() {
    this._SUPPORTED_CIPHER = this._SUPPORTED_CIPHER || Object.freeze(crypto.getCiphers())
	return this._SUPPORTED_CIPHER
  }

  static get SUPPORTED_ENCRYPTION() {
    this._SUPPORTED_CIPHER = this._SUPPORTED_ENCRYPTION || Object.freeze("TRUE","FALSE",...this.SUPPORTED_CIPHER)
	return this._SUPPORTED_ENCRYPTION
  }

  static get OUTPUT_FORMATS() {
    this._OUTPUT_FORMATS = this._OUTPUT_FORMATS || Object.freeze(["JSON","CSV","ARRAY"])
	return this._OUTPUT_FORMATS
  }
  
  static get MODES() {
    this._OUTPUT_FORMATS = this._OUTPUT_FORMATS || Object.freeze(["DDL_ONLY","DATA_ONLY","DDL_AND_DATA"])
	return this._OUTPUT_FORMATS
  }
  
  static get MACROS() {
	this._MACROS = this._MACROS || Object.freeze({ timestamp: new Date().toISOString().replace(/:/g,'.')})
	return this._MACROS
  }

}

export { YadamuConstants as default}