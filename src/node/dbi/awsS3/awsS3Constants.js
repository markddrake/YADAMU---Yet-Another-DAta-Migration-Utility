"use strict"

import HTTP from 'http'

import YadamuConstants from '../../lib/yadamuConstants.js';

class AWSS3Constants {

  static get DATABASE_KEY()               { return 'awsS3' };
  static get DATABASE_VENDOR()            { return 'AWSS3' };
  static get SOFTWARE_VENDOR()            { return 'Amazon Web Services LLC' };
  static get PROTOCOL()                   { return 's3://' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "BUCKET"                 : "yadamu"
    , "CHUNK_SIZE"             : 5 * 1024 * 1024
	, "RETRY_COUNT"            : 5
    })
    return this._STATIC_PARAMETERS;
  }

  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#_DBI_PARAMETERS
  }
  
  static get BUCKET()                     { return this.DBI_PARAMETERS.BUCKET }
  static get CHUNK_SIZE()                 { return this.DBI_PARAMETERS.CHUNK_SIZE }
  static get RETRY_COUNT()                { return this.DBI_PARAMETERS.RETRY_COUNT }
  
  static get HTTP_NAMED_STATUS_CODES()       {
	 this._HTTP_NAMED_STATUS_CODES = this._HTTP_NAMED_STATUS_CODES || (() => {
	   const httpCodes = {}
	   Object.keys(HTTP.STATUS_CODES).forEach((code) => {
		 httpCodes[HTTP.STATUS_CODES[code].toUpperCase().replace(/ /g,"_").replace(/-/g,"_").replace(/'/g,"")] = parseInt(code)
	   })
	   return Object.freeze(httpCodes)
     })();
	 return this._HTTP_NAMED_STATUS_CODES
  }
	 
  
}

export {AWSS3Constants as default }