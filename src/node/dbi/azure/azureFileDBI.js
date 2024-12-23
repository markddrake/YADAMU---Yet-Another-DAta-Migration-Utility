
import path                           from 'path'
import mime                           from 'mime-types';

import { 
  BlobServiceClient
}                                     from '@azure/storage-blob';

import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js';

import DBIConstants                   from '../base/dbiConstants.js'

import FileDBI                        from '../file/fileDBI.js'
import AzureStorageService            from './azureStorageService.js'
import AzureConstants                 from './azureConstants.js'

import {
  FileError, 
  FileNotFound, 
  DirectoryNotFound
}                                     from '../file/fileException.js'

class AzureFileDBI extends FileDBI {
 
  static #DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze({
      ...DBIConstants.DBI_PARAMETERS
    , ...AzureConstants.DBI_PARAMETERS
    })
	return this.#DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
	return AZUREFileDBI.DBI_PARAMETERS
  }

  get DATABASE_KEY()          { return AzureConstants.DATABASE_KEY + "File"};
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
 
  get FILE()                     {
	const file = super.FILE
    this._CLOUD_FILE = this._CLOUD_FILE || this.UNRESOLVED_FILE.replace(/^[A-Za-z]:\\/, '').split(path.sep).join(path.posix.sep)
	return this._CLOUD_FILE
  }
  
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

  constructor(yadamu,connectionSettings,parameters) {
	super(yadamu,connectionSettings,parameters)
  }
  
  async initialize() {
    this.cloudConnection = BlobServiceClient.fromConnectionString(this.CONNECTION_PROPERTIES.connection);
	this.cloudService = new AzureStorageService(this,{})
	await super.initialize()
  }
  
  _getInputStream() {  
    // Return the inputStream and the transform streams required to process it.
    // const stats = fs.statSync(this.FILE)
    // const fileSizeInBytes = stats.size
    this.LOGGER.info([this.DATABASE_VENDOR,YadamuConstants.READER_ROLE],`Processing file "${this.FILE}".`)
	return this.inputStream
  }

  async createInputStream() {
    const file = this.FILE
    // this.LOGGER.trace([this.constructor.name,this.DATABASE_VENDOR,],`Creating readable stream on ${file)}`)
    const stream = this.USE_ENCRYPTION ? await this.cloudService.createReadEncryptedStream(file) : await this.cloudService.createReadStream(file)
	return stream
  }
  
  async createWriteStream() {
    // this.LOGGER.trace([this.constructor.name,this.DATABASE_VENDOR,],`Creating writeable stream on ${file})`)
    const file = this.FILE
    const extension = path.extname(file);
    const contentType = mime.lookup(extension) || 'application/octet-stream'
    const outputStream = this.cloudService.createWriteStream(file,contentType,this.activeWriters)
	return outputStream
  }
  
  async loadInitializationVector() {

    let inputStream
	try {
	  inputStream = await this.createInputStream()
      const iv = new Uint8Array(this.IV_LENGTH);
      let bytesRead = 0;

      for await (const chunk of inputStream) {
        const chunkBytes = chunk.subarray(0, this.IV_LENGTH - bytesRead);
        iv.set(chunkBytes, bytesRead);
        bytesRead += chunkBytes.length;

        if (bytesRead >= this.IV_LENGTH) {
          break;
        }
      }

      if (bytesRead < this.IV_LENGTH) {
        throw new Error("Unexpected end of stream while reading Initialization Vector.");
      }
  
      this.INITIALIZATION_VECTOR = iv;
    } catch (e) {
      const cause = new FileError(this, new Error(`Unable to load Initialization Vector.`));
      cause.cause = e;
      throw cause;
    } finally {
      inputStream.destroy();
    }
  }
}	
	  
export { AzureFileDBI as default }

