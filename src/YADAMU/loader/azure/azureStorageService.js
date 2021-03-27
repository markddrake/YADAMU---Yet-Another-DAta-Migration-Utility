"use strict"

const Stream = require('stream');
const PassThrough = Stream.PassThrough;
const util = require('util')
const pipeline = util.promisify(Stream.pipeline);


const StringWriter = require('../../common/stringWriter.js');
const StringDecoderStream = require('../../common/stringDecoderStream.js');
const YadamuConstants = require('../../common/yadamuConstants.js');
const AzureConstants = require('./azureConstants.js');
const AzureError = require('./azureException.js')

class AzureStorageService {

  get CHUNK_SIZE()     { return this.parameters.CHUNK_SIZE  || AzureConstants.CHUNK_SIZE }  
  get CONTAINER() { return this._CONTAINER }
   
  constructor(blobServiceClient,container,parameters,yadamuLogger) {
	// super()
    this.blobServiceClient = blobServiceClient
	this._CONTAINER = container
	this.parameters = parameters || {}
	this.yadamuLogger = yadamuLogger

	this.containerClient = blobServiceClient.getContainerClient(this.CONTAINER);
	this.buffer = Buffer.allocUnsafe(this.CHUNK_SIZE);
	this.offset = 0;
	
	this.writeOperations = new Set()
  }
  
  isTextContent(contentType) {
    return contentType.startsWith('text/') || YadamuConstants.TEXTUAL_MIME_TYPES.includes(contentType)
  }
   
  createWriteStream(key,contentType) {
	// this.yadamuLogger.trace([this.constructor.name],`createWriteStream(${key})`)
	const passThrough = new PassThrough();
	const blockBlobClient = this.containerClient.getBlockBlobClient(key);
	const writeOperation = blockBlobClient.uploadStream(passThrough, undefined, undefined, { blobHTTPHeaders: { blobContentType: contentType}})
	this.writeOperations.add(writeOperation);
    writeOperation.then(() => {this.writeOperations.delete(writeOperation)})    
	return passThrough;
  }
  
  async createBucketContainer() {
	 
	let stack;
    try {
      stack = new Error().stack
	  const createContainerResponse = await this.containerClient.createIfNotExists();
      return createContainerResponse
	} catch (e) { 
      throw new AzureError(e,stack,`Azure.containerClient.createIfNotExists(${this.CONTAINER})`)
	}
  }

  async verifyBucketContainer() {
	 
	let stack;
    try {
      stack = new Error().stack
	  return await this.containerClient.exists()
	} catch (e) { 
      throw new AzureError(e,stack,`Azure.containerClient.createIfNotExists(${this.CONTAINER})`)
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
      throw new AzureError(e,stack,operation)
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
      throw new AzureError(e,stack,operation)
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
	const stack = new Error().stack
    try {
  	  operation = `Azure.containerClient.getBlockBlobClient(${key})`
      const blockBlobClient = this.containerClient.getBlockBlobClient(key);
  	  operation = `Azure.blockBlobClient.getProperties(${key})`
	  const props = await blockBlobClient.getProperties()
  	  operation = `Azure.blockBlobClient.download(${key})`
	  const downloadBlockBlobResponse = await blockBlobClient.download(0);
	  return downloadBlockBlobResponse.readableStreamBody.pipe(this.isTextContent(props.contentType) ? new StringDecoderStream() : new PassThrough())
	  return downloadBlockBlobResponse.readableStreamBody.pipe(new PassThrough())
	} catch (e) {
      throw new AzureError(e,stack,operation)
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
	  throw new AzureError(e,stack,operation)
    }
  }

}

module.exports = AzureStorageService;