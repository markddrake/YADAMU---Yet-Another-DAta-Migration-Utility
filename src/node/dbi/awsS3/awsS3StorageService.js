"use strict"

import path                    from 'path'
import {PassThrough}           from 'stream';

import AWSS3Constants          from './awsS3Constants.js';
import AWSS3Error              from './awsS3Exception.js'

class AWSS3StorageService {

  get CHUNK_SIZE()   { return this.parameters.CHUNK_SIZE  || AWSS3Constants.CHUNK_SIZE }  
  get RETRY_COUNT()  { return this.parameters.RETRY_COUNT ||  AWSS3Constants.RETRY_COUNT }
  
  constructor(dbi,parameters) {
	this.dbi = dbi
	this.s3Connection = this.dbi.cloudConnection
	this.parameters = parameters || {}
	this.yadamuLogger = this.dbi.yadamuLogger
	this.buffer = Buffer.allocUnsafe(this.CHUNK_SIZE);
	this.offset = 0;
  }
  
  retryOperation(retryCount) {
	return retryCount < this.RETRY_COUNT 
  }
	   
  createWriteStream(key,contentType,activeWriters) {
	// this.yadamuLogger.trace([this.constructor.name],`createWriteStream(${key})`)
  
    const params = { 
	  Bucket      : this.dbi.BUCKET
	, Key         : key
	, ContentType : contentType
	, Body        : new PassThrough()
    }	
	const writeOperation = new Promise((resolve,reject) => {
	  this.s3Connection.upload(params).send((err, data) => {
 	    if (err) {
          // this.yadamuLogger.handleException([AWSS3Constants.DATABASE_VENDOR,`FAILED`,'UPLOAD',params.Key],`Removing Active Writer`);
          this.yadamuLogger.handleException([AWSS3Constants.DATABASE_VENDOR,'UPLOAD',`FAILED`,params.Key],err);
		  reject(err)
	    } 
	    else {
          // this.yadamuLogger.trace([AWSS3Constants.DATABASE_VENDOR,'UPLOAD',`SUCCESS`,params.Key],`Removing Active Writer`);		
          activeWriters.delete(writeOperation)
		  resolve(params.Key)
	    }
        activeWriters.delete(writeOperation)
	    params.Body.destroy(err);
      })
    })	  
	activeWriters.add(writeOperation)
	return params.Body
  }

  async createBucketContainer() {
    let stack;
	let operation
    try {
      stack = new Error().stack
	  operation = `AWS.S3.headBucket(${this.dbi.BUCKET})`
	  try {
	    let results = await this.s3Connection.headBucket({Bucket : this.dbi.BUCKET}).promise();
      } catch (e) {
		if (e.statusCode === AWSS3Constants.HTTP_NAMED_STATUS_CODES.NOT_FOUND) {
          stack = new Error().stack
	      operation = `AWS.S3.createBucket(${this.dbi.BUCKET})`
    	  await this.s3Connection.createBucket({Bucket: this.dbi.BUCKET}).promise()
		  return
        } 
		throw e
	  }
	} catch (e) { 
      throw new AWSS3Error(this.dbi.DRIVER_ID,e,stack,operation)
	}
  }

  async verifyBucketContainer() {
	 
	let stack;
    try {
      stack = new Error().stack
      let results = await this.s3Connection.headBucket({Bucket : this.dbi.BUCKET}).promise();
	} catch (e) { 
      throw new AWSS3Error(this.dbi.DRIVER_ID,e,stack,`AWS.S3.headBucket(${this.dbi.BUCKET})`)
	}
  }
  
  async putObject(key,content,params) {
	params = params || {}
	params.Bucket = this.dbi.BUCKET
	params.Key = key
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
	params.Body = content
    const stack = new Error().stack
    try {
      const results = await this.s3Connection.putObject(params).promise()
	  return results;
    } catch (e) {
      throw new AWSS3Error(this.dbi.DRIVER_ID,e,stack,`AWS.S3.putBucket(s3://${this.dbi.BUCKET}/${params.Key})`)
	}
  }
  
  async getObject(key,params) {
	params = params || {}
	params.Bucket = this.dbi.BUCKET
	params.Key = key
    const stack = new Error().stack
    try {
      const results = await this.s3Connection.getObject(params).promise()
	  return results;
    } catch (e) {
      throw new AWSS3Error(this.dbi.DRIVER_ID,e,stack,`AWS.S3.getObject(s3://${this.dbi.BUCKET}/${params.Key})`)
	}
  }

  async getObjectProps(key,params) {
	 
	params = params || {}
	params.Bucket = this.dbi.BUCKET
	params.Key = key

	let retryCount =  0
    const stack = new Error().stack

	while (true) {
      try {
        return await this.s3Connection.headObject(params).promise()
	  } catch (e) {
	    const awsError = new AWSS3Error(this.dbi.DRIVER_ID,e,stack,`AWS.S3.headObject(s3://${this.dbi.BUCKET}/${params.Key})`)
        if (awsError.FileNotFound() && this.retryOperation(retryCount)) { 
		  await new Promise((resolve,reject) => {
		    setTimeout(() => {resolve()},e.retryDelay)
	      })
		  retryCount++
		  continue
		}
	    throw awsError	  
      }
	}
  }

  async createReadStream(key,params) {
	  
	// this.yadamuLogger.trace([this.constructor.name],`createReadStream(${key})`)
	
	params = params || {}
	params.Bucket = this.dbi.BUCKET
	params.Key = key
    const stack = new Error().stack
    try {
      const stream = await this.s3Connection.getObject(params).createReadStream()
	  return stream;
	} catch (e) {
	  throw new AWSS3Error(this.dbi.DRIVER_ID,e,stack,`AWS.S3.getObject(s3://${this.dbi.BUCKET}/${params.Key})`)
	}
  }
  
  async deleteFolder(key,params) {
	 
	params = params || {}
	params.Bucket = this.dbi.BUCKET
	params.Prefix = key.split(path.sep).join(path.posix.sep)
	let stack
	let folder
    do { 
      try {
		stack = new Error().stack
	    folder = await this.s3Connection.listObjectsV2(params).promise();
      } catch (e) {
        throw new AWSS3Error(this.dbi.DRIVER_ID,e,stack,`AWS.S3.listObjectsV2(s3://${this.dbi.BUCKET}/${params.Key})`)
	  }
	  if (folder.Contents.length === 0) break;
	  const deleteParams = {
		Bucket : this.dbi.BUCKET
	  , Delete : { Objects : folder.Contents.map((c) => { return {Key : c.Key }})}
	  }
	  try {
  	    stack = new Error().stack
	    await this.s3Connection.deleteObjects(deleteParams).promise();
      } catch (e) {
        throw new AWSS3Error(this.dbi.DRIVER_ID,e,stack,`AWS.S3.deleteObjects(s3://${this.dbi.BUCKET}/${deleteParams.Delete.Objects.length})`)
	  }
	} while (folder.IsTruncated);
  }

}

export { AWSS3StorageService as default }