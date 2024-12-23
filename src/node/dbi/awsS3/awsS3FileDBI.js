
import path                           from 'path'
import mime                           from 'mime-types';

import { 
  S3Client
}                                     from "@aws-sdk/client-s3";


import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js';

import DBIConstants                   from '../base/dbiConstants.js'

import FileDBI                        from '../file/fileDBI.js'
import AWSS3StorageService            from './awsS3StorageService.js'
import AWSS3Constants                 from './awsS3Constants.js'

import {
  FileError, 
  FileNotFound, 
  DirectoryNotFound
}                                     from '../file/fileException.js'

class AWSS3FileDBI extends FileDBI {
 
  static #DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze({
      ...DBIConstants.DBI_PARAMETERS
    , ...AWSS3Constants.DBI_PARAMETERS
    })
	return this.#DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
	return AWSS3FileDBI.DBI_PARAMETERS
  }

  get DATABASE_KEY()          { return AWSS3Constants.DATABASE_KEY + "File"};
  get DATABASE_VENDOR()       { return AWSS3Constants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()       { return AWSS3Constants.SOFTWARE_VENDOR};
  get PROTOCOL()              { return AWSS3Constants.PROTOCOL }
  
  get BUCKET() {
    this._BUCKET = this._BUCKET || (() => { 
	  const bucket = this.parameters.BUCKET || AWSS3Constants.BUCKET
	  return YadamuLibrary.macroSubstitions(bucket, this.yadamu.MACROS)
	})();
	return this._BUCKET
  }

  get STORAGE_ID()            { return this.BUCKET }
  
  get FILE()                     {
	const file = super.FILE
    this._CLOUD_FILE = this._CLOUD_FILE || this.UNRESOLVED_FILE.replace(/^[A-Za-z]:\\/, '').split(path.sep).join(path.posix.sep)
	return this._CLOUD_FILE
  }
  
  redactPasswords() {
	 const connectionProperties = super.redactPasswords()
	 connectionProperties.connection.credentials.secretAccessKey = '#REDACTED'
     return connectionProperties
  }
  
  addVendorExtensions(connectionProperties) {
 
	let url = connectionProperties.endpoint
	url = url && url.indexOf('://') < 0 ? `http://${url}` : url
	try {
	  url = new URL(url ? url : 'http://0.0.0.0')
	} catch (e) {
      this.LOGGER.error([this.DATABASE_VENDOR,'CONNECTION'],`Invalid endpoint specified: "${connectionProperties.endpoint}"`)
	  this.LOGGER.handleException([this.DATABASE_VENDOR,'CONNECTION'],e)
	  url = new URL('http://0.0.0.0')
	}

    url.protocol                          = this.parameters.PROTOCOL  || url.protocol 
	url.hostname                          = this.parameters.HOSTNAME  || url.hostname
	url.port                              = this.parameters.PORT      || url.port
	url                                   = url.toString()
	
	connectionProperties.accessKeyId      = this.parameters.USERNAME  || connectionProperties.accessKeyId 
    connectionProperties.secretAccessKey  = this.parameters.PASSWORD  || connectionProperties.secretAccessKey	
	connectionProperties.region           = this.parameters.REGION    || connectionProperties.region	         // this.REGION
    connectionProperties.s3ForcePathStyle = true
    connectionProperties.signatureVersion = "v4"
    connectionProperties.endpoint         = url

    connectionProperties.directory       =  connectionProperties.directory || '/'	
	
	connectionProperties.connection = {
      region             : connectionProperties.region
	, forcePathStyle     : connectionProperties.s3ForcePathStyle
	, endpoint           : connectionProperties.endpoint
	, signatureVersion   : connectionProperties.signatureVersion
	, credentials        : {
	    accessKeyId      : connectionProperties.accessKeyId
	  , secretAccessKey  : connectionProperties.secretAccessKey  
	  }
	}
	
	return connectionProperties

  }

  constructor(yadamu,connectionSettings,parameters) {
	super(yadamu,connectionSettings,parameters)
  }
  
  async initialize() {
	this.cloudConnection = await new S3Client(this.CONNECTION_PROPERTIES.connection)
	this.cloudService = new AWSS3StorageService(this,{})
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
    const stream = await this.cloudService.createReadStream(file)
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
	  
export { AWSS3FileDBI as default }

