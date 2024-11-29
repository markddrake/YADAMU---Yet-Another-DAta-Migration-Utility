
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

import DatabaseError                  from './azureException.js'
							          							          
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

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze({
      ...DBIConstants.DBI_PARAMETERS
    , ...AzureConstants.DBI_PARAMETERS
    })
	return this.#DBI_PARAMETERS
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
 
  redactPasswords() {
   
    const connectionProperties = structuredClone(this.CONNECTION_PROPERTIES)
	
	const keyValues =  connectionProperties.connection.split(';') 
  	
    const keyValue = keyValues[2].split('=')
	keyValue[1] = '#REDACTED'
	keyValues[2] = keyValue.join('=')

    connectionProperties.connection = keyValues.join(';')
	connectionProperties.password = '#REDACTED'
	return connectionProperties
	
  }
   
  addVendorExtensions(connectionProperties) {

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
		

	const keyValues = typeof connectionProperties.connection  === 'string' ? connectionProperties.connection.split(';') : ['DefaultEndpointsProtocol=','AccountName=','AccountKey=','BlobEndpoint=']
	
	let keyValue = keyValues[0].split('=')
	keyValue[1] = this.parameters.PROTOCOL || keyValue[1]
	keyValues[0] = keyValue.join('=')
	
	keyValue = keyValues[1].split('=')
	keyValue[1] = this.parameters.USERNAME || keyValue[1]
	connectionProperties.user = keyValue[1]
	keyValues[1] = keyValue.join('=')
	
    keyValue = keyValues[2].split('=')
	keyValue[1] = this.parameters.PASSWORD || keyValue[1]
	connectionProperties.password = keyValue[1]
	keyValues[2] = keyValue.join('=')

    keyValue = keyValues[3].split('=')
	
	let url = keyValue[1]
	try {
	  url = new URL(url ? url : 'http://0.0.0.0')
	} catch (e) {
      this.logger.error([this.DATABASE_VENDOR,'CONNECTION'],`Invalid endpoint specified: "${connectionProperties}"`)
	  this.LOGGER.handleException([this.DATABASE_VENDOR,'CONNECTION'],e)
	  url = new URL('http://0.0.0.0')
	}

    url.protocol                      = this.parameters.PROTOCOL  || url.protocol 
    connectionProperties.protocol     = url.protocol 
	url.hostname                      = this.parameters.HOSTNAME || url.hostname
    connectionProperties.host         = url.hostname
	url.port                          = this.parameters.PORT || url.port
    connectionProperties.port         = url.port
    connectionProperties.database     = url.pathname
	url                               = url.toString()

	
	keyValue[1] = url
	keyValues[3] = keyValue.join('=')
	  

	const connectionInfo = { 
	  connection : keyValues.join(';')
	}
	
	connectionProperties = { 
	   ...connectionProperties,
	   ...connectionInfo
	}
	
	return connectionProperties
	
  }
  
  constructor(yadamu,manager,connectionSettings,parameters) {
    // Export File Path is a Directory for in Load/Unload Mode
    super(yadamu,manager,connectionSettings,parameters)
	
	this.DATABASE_ERROR = DatabaseError
  }   
  
  async createConnectionPool() {
	// this.LOGGER.trace([this.constructor.name],`BlobServiceClient.fromConnectionString()`)
    this.cloudConnection = BlobServiceClient.fromConnectionString(this.CONNECTION_PROPERTIES.connection);
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