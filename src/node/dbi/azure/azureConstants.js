
import HTTP from 'http'

import YadamuConstants from '../../lib/yadamuConstants.js';

class AzureConstants {

  
  static get DATABASE_KEY()               { return 'azure' };
  static get DATABASE_VENDOR()            { return 'AzureBlobStorage' };
  static get SOFTWARE_VENDOR()            { return 'Microsoft Corporation' };
  static get PROTOCOL()                   { return 'azure://' };

  static get STATIC_PARAMETERS() { 
    this._STATIC_PARAMETERS = this._STATIC_PARAMETERS || Object.freeze({
      "CONTAINER"              : "yadamu"
    , "CHUNK_SIZE"             : 8 * 1024 * 1024
    })
    return this._STATIC_PARAMETERS;
  }

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS() { 
    this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze(Object.assign({RDBMS: this.DATABASE_KEY},this.STATIC_PARAMETERS,YadamuConstants.YADAMU_CONFIGURATION[this.DATABASE_KEY] || {}))
    return this.#DBI_PARAMETERS
  }
  
  static get CONTAINER()                  { return this.DBI_PARAMETERS.CONTAINER }
  static get CHUNK_SIZE()                 { return this.DBI_PARAMETERS.CHUNK_SIZE }

  
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

export {AzureConstants as default }