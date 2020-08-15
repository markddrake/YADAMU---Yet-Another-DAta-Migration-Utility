"use strict"

const HTTP = require('http')

const YadamuConstants = require('../../common/yadamuConstants.js');

class S3Constants {

  static get S3_DEFAULTS() { 
    this._S3_DEFAULTS = this._S3_DEFAULTS || Object.freeze({
      "BUCKET_NAME"            : "yadamu-data-staging"
    , "CHUNK_SIZE"             : 5 * 1024 * 1024
    })
    return this._S3_DEFAULTS;
  }

  static get DEFAULT_PARAMETERS() { 
    this._DEFAULT_PARAMETERS = this._DEFAULT_PARAMETERS || Object.freeze(Object.assign({},this.S3_DEFAULTS,YadamuConstants.YADAMU_DEFAULTS.s3 || {}))
    return this._DEFAULT_PARAMETERS
  }
  
  static get BUCKET_NAME()                { return this.DEFAULT_PARAMETERS.BUCKET_NAME }
  static get CHUNK_SIZE()                 { return this.DEFAULT_PARAMETERS.CHUNK_SIZE }
  static get DATABASE_VENDOR()            { return 'AWSS3' };
  static get SOFTWARE_VENDOR()            { return 'Amazon Web Services LLC' };
  
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

module.exports = S3Constants;