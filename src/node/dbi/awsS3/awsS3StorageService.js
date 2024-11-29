
import path                   from 'path'

import {
  PassThrough
}                             from 'stream';

import {
  setTimeout 
}                             from "timers/promises"

import { 
  pipeline 
}                               from 'stream/promises';

import { 
  HeadBucketCommand
, CreateBucketCommand
, DeleteObjectsCommand
, GetObjectCommand
, HeadObjectCommand
, ListObjectsV2Command
, PutObjectCommand
}                             from "@aws-sdk/client-s3";

import { 
  Upload 
}                             from "@aws-sdk/lib-storage";

import StringWriter           from '../../util/stringWriter.js';

import AWSS3Constants         from './awsS3Constants.js';

class ByteCounter extends PassThrough {

  constructor(input) {
    super(input) 
	this.byteCount = 0
  }
  
  _transform (data, enc, callback) {
	this.byteCount+= data.length
	this.push(data)
    callback()
  }
  
  _flush(callback) {
	// console.log('Bytes Written:',this.byteCount)
    callback()
  }
}
 
class AWSS3StorageService {

  get CHUNK_SIZE()   { return this.parameters.CHUNK_SIZE  || AWSS3Constants.CHUNK_SIZE }  
  get RETRY_COUNT()  { return this.parameters.RETRY_COUNT ||  AWSS3Constants.RETRY_COUNT }
  
  get LOGGER()             { return this._LOGGER }
  set LOGGER(v)            { this._LOGGER = v }
  
  get DEBUGGER()           { return this._DEBUGGER }
  set DEBUGGER(v)          { this._DEBUGGER = v }

  constructor(dbi,parameters) {
	this.dbi = dbi
	this.s3Client = this.dbi.cloudConnection
	this.parameters = parameters || {}
	this.LOGGER = this.dbi.LOGGER
	this.buffer = Buffer.allocUnsafe(this.CHUNK_SIZE);
	this.offset = 0;
  }
  
  retryOperation(retryCount) {
	return retryCount < this.RETRY_COUNT 
  }
  	   
  createWriteStream(key,contentType,activeWriters) {
	
	// this.LOGGER.trace([AWSS3Constants.DATABASE_VENDOR,'UPLOAD'],key);
  
  
    const input = { 
	  client     : this.s3Client
	, params     : { 
	    Bucket      : this.dbi.BUCKET
	  , Key         : key
	  , ContentType : contentType
	  , queueSize   : this.dbi.parameters.PARALLEL  
	  , Params      : 'new PassThrough()'
	  }
    }
        
	const operation = `S3LibStorage.Upload(${JSON.stringify(input.params)})`
	
	input.params.Body = new PassThrough()
	
    const stack = new Error().stack

	const writeOperation = new Promise((resolve,reject) => {
      const upload = new Upload(input)	
      upload.done().then(() => {
	    // this.LOGGER.trace([AWSS3Constants.DATABASE_VENDOR,'UPLOAD',`DONE`],input.params.Key);
	    input.params.Body.destroy()
	    resolve(input.params.Key)
	  }).catch((err) => {

        const cause = this.dbi.getDatabaseException(err,stack,operation)
        this.LOGGER.handleException([AWSS3Constants.DATABASE_VENDOR,'UPLOAD',`FAILED`,input.params.Key],err);
        input.params.Body.destroy(err)
	    reject(cause)
	  }).finally(() => {
	    // this.LOGGER.trace([AWSS3Constants.DATABASE_VENDOR,'UPLOAD',`FINAL`],input.params.Key);
	    activeWriters.delete(writeOperation)
	  })
    })
	
	activeWriters.add(writeOperation)
	return input.params.Body
  }
  
  async createBucketContainer() {
    let stack;
	let operation;
    const input = {Bucket : this.dbi.BUCKET}
    try {
	  try {
        operation = `S3Client.HeadBucketCommand(${JSON.stringify(input)})`
		const command = new HeadBucketCommand(input)
	    stack = new Error().stack
	    let output = await this.s3Client.send(command)
      } catch (e) {
		const cause = this.dbi.getDatabaseException(e,stack,operation) 
		if (cause.notFound()) {
	      operation = `S3Client.CreateBucketCommand(${JSON.stringify(input)})`
		  const command = new CreateBucketCommand (input)
          stack = new Error().stack
          let output = await this.s3Client.send(command)
     	  return output
        } 
		throw cause
	  }
	} catch (e) { 
      throw this.dbi.getDatabaseException(e,stack,operation)
	}
  }

  async verifyBucketContainer() {
	 
    let stack;
    const input = {Bucket : this.dbi.BUCKET}
	const operation = `S3Client.HeadBucketCommand(${JSON.stringify(input)})`

    try {
      const command = new HeadBucketCommand(input)
      stack = new Error().stack
	  const output = await this.s3Client.send(command)	 
	  return output
	} catch (e) { 
      throw this.dbi.getDatabaseException(e,stack,operation)
	}
  }
  
  async putObject(key,content,input) {
	input = input || {}
	input.Bucket = this.dbi.BUCKET
	input.Key = key
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
	let stack
	const operation = `S3Client.PutObjectCommand(${JSON.stringify(input)})`
	input.Body = content
    try {
      const command = new PutObjectCommand(input)
      stack = new Error().stack
	  const output = await this.s3Client.send(command)	 
	  return output;
    } catch (e) {
      throw this.dbi.getDatabaseException(e,stack,operation)
	}
  }
    
  async getObject(key,input) {
	input = input || {}
	input.Bucket = this.dbi.BUCKET
	input.Key = key
	
	let stack
	const operation = `S3Client.GetObjectCommand(${JSON.stringify(input)})`
    try {
      const command = new GetObjectCommand(input)
      stack = new Error().stack
	  const output = await this.s3Client.send(command)	 
	  return output;
    } catch (e) {
      throw this.dbi.getDatabaseException(e,stack,operation)
	}
  }

  
  async getContentAsString(key,input) {
	const object = await this.getObject(key,input)
    const sw = new StringWriter();
	await pipeline(object.Body,sw)
	return sw.toString();
  }
  
  async getObjectProps(key,input) {
	 
	input = input || {}
	input.Bucket = this.dbi.BUCKET
	input.Key = key

	let retryCount =  0
	const operation = `S3Client.HeadObjectCommand(${JSON.stringify(input)})`
    const command = new HeadObjectCommand(input)
        
	while (true) {
      try {
        const stack = new Error().stack
	    const output = await this.s3Client.send(command)	 
  	    return output;
	  } catch (e) {
	    const awsError = this.dbi.getDatabaseException(e,stack,operation)
        if (awsError.notFound() && this.retryOperation(retryCount)) { 
		  await setTimeout(e.retryDelay)
		  retryCount++
		  continue
		}
	    throw awsError	  
      }
	}
  }

  async createReadStream(key,input) {
	  
	// this.LOGGER.trace([this.constructor.name],`createReadStream(${key})`)
	
	const object = await this.getObject(key,input)
	return object.Body

  }
  
  async deleteFolder(key,input) {
	 
	input = input || {}
	input.Bucket = this.dbi.BUCKET
	input.Prefix = key.split(path.sep).join(path.posix.sep)
	let stack
	let operation
	let folder
    do { 
      try {
   	    operation = `S3Client.ListObjectsV2Command(${JSON.stringify(input)})`
        const command = new ListObjectsV2Command(input)
		stack = new Error().stack
	    folder = await this.s3Client.send(command)	 
      } catch (e) {
        throw this.dbi.getDatabaseException(e,stack,operation)
	  }
	  if (folder.KeyCount === 0) break;
	  const deleteinput = {
		Bucket : this.dbi.BUCKET
	  , Delete : { Objects : folder.Contents.map((c) => { return {Key : c.Key }})}
	  }
	  try {
  	    stack = new Error().stack
   	    const operation = `S3Client.ListObjectsV2Command(${JSON.stringify(deleteinput)})`
        const command = new DeleteObjectsCommand(deleteinput)
        await this.s3Client.send(command)	 
      } catch (e) {
        throw this.dbi.getDatabaseException(e,stack,operation)
	  }
	} while (folder.IsTruncated);
  }

  async deleteFile(key,input) {
	 
	/* 
	input = input || {}
	input.Bucket = this.dbi.BUCKET
	input.Prefix = key.split(path.sep).join(path.posix.sep)
    const deleteinput = {
	  Bucket : this.dbi.BUCKET
	, Delete : { Objects : folder.Contents.map((c) => { return {Key : c.Key }})}
	}
	let stack
    try {
      stack = new Error().stack
	  await this.s3Client.deleteObjects(deleteinput).promise();
    } catch (e) {
      throw this.dbi.getDatabaseException(e,stack,`S3Client.deleteObjects(s3://${this.dbi.BUCKET}/${deleteinput.Delete.Objects.length})`)
	}
	*/
  }

}

export { AWSS3StorageService as default }