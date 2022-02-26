"use strict"

import { 
  compose,
  PassThrough
}                               from 'stream';

import { 
  pipeline 

}                               from 'stream/promises';

import { 
  setTimeout 
}                               from 'timers/promises'

import StringWriter             from '../../util/stringWriter.js';
import StringDecoderStream      from '../../util/stringDecoderStream.js';
import YadamuConstants          from '../../lib/yadamuConstants.js';
import AzureConstants           from './azureConstants.js';
import AzureError               from './azureException.js'
import NullWritable             from '../../util/nullWritable.js';

class ByteCounter extends PassThrough {

  constructor(options) {
    super(options) 
	this.byteCount = 0
  }
  
  _transform (data, enc, callback) {
	this.byteCount+= data.length
	this.push(data)
    callback()
  }
  
  _flush(callback) {
	console.log('Bytes Written:',this.byteCount)
    callback()
  }
}
 
class AzureStorageService {

  get CHUNK_SIZE()     { return this.parameters.CHUNK_SIZE  || AzureConstants.CHUNK_SIZE }  
   
  constructor(dbi,parameters) {
	this.dbi = dbi
	this.blobServiceClient = dbi.cloudConnection
	this.yadamuLogger = dbi.yadamuLogger
	this.parameters = parameters || {}

	this.containerClient = this.blobServiceClient.getContainerClient(this.dbi.CONTAINER);
	this.buffer = Buffer.allocUnsafe(this.CHUNK_SIZE);
	this.offset = 0;
	
  }
  
  isTextContent(contentType) {
    return contentType.startsWith('text/') || YadamuConstants.TEXTUAL_MIME_TYPES.includes(contentType)
  }
   
  createWriteStream(key,contentType,activeWriters) {
	// this.yadamuLogger.trace([this.constructor.name],`createWriteStream(${key})`)
	// const passThrough =  new ByteCounter()
	const passThrough =  new PassThrough()
	const blockBlobClient = this.containerClient.getBlockBlobClient(key);
	const writeOperation = blockBlobClient.uploadStream(passThrough, undefined, undefined, { blobHTTPHeaders: { blobContentType: contentType}})
	activeWriters.add(writeOperation);
    writeOperation.then(() => {
      // this.yadamuLogger.trace([AzureConstants.DATABASE_VENDOR,'UPLOAD',`SUCCESS`,key],``);		
      activeWriters.delete(writeOperation)
	}).catch((err) => {
      // this.yadamuLogger.trace([AzureConstants.DATABASE_VENDOR,'UPLOAD',`FAILED`,key],``);		
      this.yadamuLogger.handleException([AzureConstants.DATABASE_VENDOR,'UPLOAD',`FAILED`,key],err);
    }).finally(() => {
      // this.yadamuLogger.trace([AzureConstants.DATABASE_VENDOR,'UPLOAD',key],`Removing Active Writer`);		
      activeWriters.delete(writeOperation)
	})
	return compose(passThrough, new NullWritable())
  }
  
  async createBucketContainer() {
	 
	let stack;
    try {
      stack = new Error().stack
	  const createContainerResponse = await this.containerClient.createIfNotExists(this.dbi.CONTAINER);
      return createContainerResponse
	} catch (e) { 
      throw new AzureError(this.dbi.DRIVER_ID,e,stack,`Azure.containerClient.createIfNotExists(${this.dbi.CONTAINER})`)
	}
  }

  async verifyBucketContainer() {
	 
	let stack;
    try {
      stack = new Error().stack
	  return await this.containerClient.exists()
	} catch (e) { 
      throw new AzureError(this.dbi.DRIVER_ID,e,stack,`Azure.containerClient.createIfNotExists(${this.dbi.CONTAINER})`)
	}
  }
  
  async putObject(key,content) {
	let operation
	const stack = new Error().stack
    try {
  	  operation = `Azure.containerClient.getBlockBlobClient(${key})`
      const blockBlobClient = this.containerClient.getBlockBlobClient(key);
      switch (true) {
	    case Buffer.isBuffer(content):
	      break;
       case (typeof content === 'object'):
	     content = JSON.stringify(content);
	   case (typeof content === 'string'):
	     content = Buffer.from(content);
         break;
	   default:
	  }
  	  operation = `Azure.blockBlobClient.upload(${key})`
	  const uploadBlobResponse = await blockBlobClient.upload(content, content.length);
	  return uploadBlobResponse
	} catch (e) {
      throw new AzureError(this.dbi.DRIVER_ID,e,stack,operation)
	}
  }
  
  async getObjectProps(key,params) {
	let operation
	const stack = new Error().stack
    try {
  	  operation = `Azure.containerClient.getBlockBlobClient(${key})`
      const blockBlobClient = this.containerClient.getBlockBlobClient(key);
  	  operation = `Azure.blockBlobClient.getProperties(${key})`
      const properties = await blockBlobClient.getProperties();
	  return properties
	} catch (e) {
      throw new AzureError(this.dbi.DRIVER_ID,e,stack,operation)
	}
  }

  async getObject(key,params) {
     const is = await this.createReadStream(key,params) 
	 const sw = new StringWriter();
	 await pipeline(is,sw)
	 return sw.toString();
  }

  async createReadStream(key,params) {
	  
	let operation
    let retryOn404 = true
	const stack = new Error().stack
	while (true) {
      try {
		operation = `Azure.containerClient.getBlockBlobClient(${key})`
        const blockBlobClient = this.containerClient.getBlockBlobClient(key);
  	    operation = `Azure.blockBlobClient.getProperties(${key})`
	    const props = await blockBlobClient.getProperties()
  	    operation = `Azure.blockBlobClient.download(${key})`
	    const downloadBlockBlobResponse = await blockBlobClient.download(0);
		return compose(downloadBlockBlobResponse.readableStreamBody,this.isTextContent(props.contentType) ? new StringDecoderStream() : new PassThrough())
	  } catch (e) {
		if ((retryOn404) && (e.statusCode && (e.statusCode === 404))) {
   		  retryOn404 = false
  		  await setTimeout(100)
		  this.yadamuLogger.qaWarning([AzureConstants.DATABASE_VENDOR,'READ',`RETRY`,key],`Retrying after 404`);		
		  continue
		}
	    throw new AzureError(this.dbi.DRIVER_ID,e,stack,operation)
	  }
	}
  }
  
  async deleteFolder(key,params) {

    let stack	 
	let operation 
    try {
      stack = new Error().stack
	  operation = `Azure.containerClient.listBlobsFlat("${key}")`
	  for await (const blob of this.containerClient.listBlobsFlat({prefix: key})) {
        operation = `Azure.containerClient.deleteBlob("${blob.name}")`
	    this.containerClient.deleteBlob(blob.name, {deleteSnapshots : "include"})
	  }  
    } catch (e) {
	  throw new AzureError(this.dbi.DRIVER_ID,e,stack,operation)
    }
  }

}

export {AzureStorageService as default }