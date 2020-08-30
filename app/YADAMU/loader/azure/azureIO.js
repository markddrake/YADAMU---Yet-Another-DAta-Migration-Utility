"use strict"

const Stream = require('stream');
const util = require('util')
const pipeline = util.promisify(Stream.pipeline);

const Transform = require('stream').Transform
const PassThrough = require('stream').PassThrough;
const StringWriter = require('../../common/StringWriter.js');
const AzureConstants = require('./azureConstants.js');
const AzureError = require('./azureError.js')

class AzureIO extends Transform{

  get CHUNK_SIZE()     { return this.parameters.CHUNK_SIZE  || AzureConstants.CHUNK_SIZE }  
  get CONTAINER_NAME() { return this.parameters.CONTAINER_NAME || AzureConstants.CONTAINER_NAME }
  
  constructor(blobServiceClient,parameters,yadamuLogger) {
	super()
    this.blobServiceClient = blobServiceClient
	this.parameters = parameters || {}
	this.yadamuLogger = yadamuLogger

	this.containerClient = blobServiceClient.getContainerClient(this.CONTAINER_NAME);
	this.buffer = Buffer.allocUnsafe(this.CHUNK_SIZE);
	this.offset = 0;
  }
  
  createWriteStream(key) {
	// this.yadamuLogger.trace([this.constructor.name],`createWriteStream(${key})`)
	const passThrough = new PassThrough();
    const blockBlobClient = this.containerClient.getBlockBlobClient(key);
	blockBlobClient.uploadStream(passThrough);
    return passThrough;
  }
  
  async verifyContainer() {
	 
	let stack;
    try {
      stack = new Error().stack
	  const createContainerResponse = await this.containerClient.createIfNotExists();
      return createContainerResponse
	} catch (e) { 
      throw new AzureError(e,stack,`Azure.containerClient.createIfNotExists(${this.CONTAINER_NAME})`)
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
	  console.log(e)
      throw new S3Error(e,stack,operation)
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
  	  operation = `Azure.blockBlobClient.download(${key})`
      const downloadBlockBlobResponse = await blockBlobClient.download(0);
	  return downloadBlockBlobResponse.readableStreamBody
	} catch (e) {
	  console.log(e)
      throw new S3Error(e,stack,operation)
	}
  }
      
  _transform(data,enc,callback) {
    if (this.offset + data.length > this.CHUNK_SIZE) {
      this.push(this.buffer.slice(0,this.offset),undefined,() => {
        this.emit('bufferWrite')
	  })
  	  this.buffer = Buffer.allocUnsafe(this.CHUNK_SIZE);
      this.offset = 0
	}
	this.offset+= chunk.copy(this.buffer,this.offset,0)
  }

  async _final() {
	// this.yadamuLogger.trace([this.constructor.name],`_final(${this.offset})`)
	await new Promise((resolve,reject) => {
      this.push(this.buffer.slice(0,this.offset),undefined,() => {
        this.emit('bufferWrite')
		resolve() })
	})
	this.offset = 0
  }
}

module.exports = AzureIO;