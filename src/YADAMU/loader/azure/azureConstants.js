"use strict"

const HTTP = require('http')

const YadamuConstants = require('../../common/yadamuConstants.js');

class AzureConstants {

  static get AZURE_DEFAULTS() { 
    this._AZURE_DEFAULTS = this._AZURE_DEFAULTS || Object.freeze({
      "CONTAINER"              : "yadamu"
    , "CHUNK_SIZE"             : 8 * 1024 * 1024
    })
    return this._AZURE_DEFAULTS;
  }

  static get DEFAULT_PARAMETERS() { 
    this._DEFAULT_PARAMETERS = this._DEFAULT_PARAMETERS || Object.freeze(Object.assign({},this.AZURE_DEFAULTS,YadamuConstants.YADAMU_DEFAULTS.azure || {}))
    return this._DEFAULT_PARAMETERS
  }
  
  static get AZURITE_CONNECT_STRNG()      { `DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://192.168.1.250:10000/devstoreaccount1;` }

  static get CONTAINER()                  { return this.DEFAULT_PARAMETERS.CONTAINER }
  static get CHUNK_SIZE()                 { return this.DEFAULT_PARAMETERS.CHUNK_SIZE }
  static get DATABASE_VENDOR()            { return 'AzureBlobStorage' };
  static get SOFTWARE_VENDOR()            { return 'Microsoft Corporation' };

}

module.exports = AzureConstants;