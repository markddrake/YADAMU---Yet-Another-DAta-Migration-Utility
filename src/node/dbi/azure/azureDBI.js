
import path                           from 'path';

import { 
  performance 
}                                     from 'perf_hooks';

/* Database Vendors API */                                    

import { 
  BlobServiceClient
}                                     from '@azure/storage-blob';

/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

/* Yadamu DBI */                                    

import CloudDBI                       from '../cloud/cloudDBI.js';
import DBIConstants                   from '../base/dbiConstants.js'

/* Vendor Specific DBI Implimentation */                                    
							          							          
import AzureConstants                 from './azureConstants.js';
import AzureStorageService            from './azureStorageService.js';

class AzureDBI extends CloudDBI {
 
  /*
  **
  ** Extends CloudDBI enabling operations on Azure Blob Containers rather than a local file system.
  ** 
  ** !!! Make sure your head is wrapped around the following statements before touching this code.
  **
  ** An Export operaton involves reading data from the Azure Blob store
  ** An Import operation involves writing data to the Azure Blob store
  **
  */

  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.DBI_PARAMETERS,AzureConstants.DBI_PARAMETERS))
	return this.#_DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
	return AzureDBI.DBI_PARAMETERS
  }

  get DATABASE_KEY()          { return AzureConstants.DATABASE_KEY};
  get DATABASE_VENDOR()       { return AzureConstants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()       { return AzureConstants.SOFTWARE_VENDOR};
  get PROTOCOL()              { return AzureConstants.PROTOCOL }

  get CONTAINER() {
    this._CONTAINER = this._CONTAINER || (() => { 
	  const container = this.parameters.CONTAINER || AzureConstants.CONTAINER
	  this._CONTAINER = YadamuLibrary.macroSubstitions(container, this.yadamu.MACROS)
	  return this._CONTAINER
	})();
	return this._CONTAINER
  }
  
  get STORAGE_ID() { return this.CONTAINER }
													
  constructor(yadamu,manager,connectionSettings,parameters) {
    // Export File Path is a Directory for in Load/Unload Mode
    super(yadamu,manager,connectionSettings,parameters)
  }   
  
  createDatabaseError(driverId,cause,stack,sql) {
    return new AzureError(driverId,cause,stack,sql)
  }
  
  getVendorProperties() {

    /*
	**
	** Connection is described by a single object made up of key value pairs seperated by semi-colons ';', E.G.
	**
	** "DefaultEndpointsProtocol=http;AccountName=${this.properties.USERNAME};AccountKey=${this.properties.PASSWORD};BlobEndpoint=${this.properties.HOSTNAME}:${this.properties.PORT}/${this.properties.USERNAME}"
	** 
	** DefaultEndpointsProtocol=http;
	** AccountName=devstoreaccount1;
	** AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;
	** BlobEndpoint=http://yadamu-db1:10000/devstoreaccount1"
	**
	*/
	
	const keyValues = this.vendorProperties = typeof this.vendorProperties  === 'string' ? this.vendorProperties.split(';') : ['DefaultEndpointsProtocol=','AccountName=','AccountKey=','BlobEndpoint=']
	
	let keyValue = keyValues[0].split('=')
	keyValue[1] = this.parameters.PROTOCOL || keyValue[1]
	keyValues[0] = keyValue.join('=')
	
	keyValue = keyValues[1].split('=')
	keyValue[1] = this.parameters.USERNAME || keyValue[1]
	keyValues[1] = keyValue.join('=')
	
    keyValue = keyValues[2].split('=')
	keyValue[1] = this.parameters.PASSWORD || keyValue[1]
	keyValues[2] = keyValue.join('=')

    keyValue = keyValues[3].split('=')
	
	let url = keyValue[1]
	try {
	  url = new URL(url ? url : 'http://0.0.0.0')
	} catch (e) {
      this.logger.error([this.DATABASE_VENDOR,'CONNECTION'],`Invalid endpoint specified: "${vendorProperties.endpoint}"`)
	  this.LOGGER.handleException([this.DATABASE_VENDOR,'CONNECTION'],e)
	  url = new URL('http://0.0.0.0')
	}

    url.protocol                      = this.parameters.PROTOCOL  || url.protocol 
	url.hostname                      = this.parameters.HOSTNAME || url.hostname
	url.port                          = this.parameters.PORT || url.port
	url                               = url.toString()
	
	keyValue[1] = url
	keyValues[3] = keyValue.join('=')

	return keyValues.join(';');

  }

  async createConnectionPool() {
	// this.LOGGER.trace([this.constructor.name],`BlobServiceClient.fromConnectionString()`)
    this.cloudConnection = BlobServiceClient.fromConnectionString(this.vendorProperties);
	this.cloudService = new AzureStorageService(this,{})
  }
  
  getContentLength(props) {
    return props.contentLength
  }

  classFactory(yadamu) {
	return new AzureDBI(yadamu,this,this.connectionParameters,this.parameters)
  }
    
}

export {AzureDBI as default }