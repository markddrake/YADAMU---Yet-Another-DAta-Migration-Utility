"use strict"

const {PassThrough, finished} = require('stream');
const path = require('path')
const AWSS3Constants = require('./awsS3Constants.js');
const AWSS3Error = require('./awsS3Exception.js')

class AWSS3StorageService {

  get CHUNK_SIZE()   { return this.parameters.CHUNK_SIZE  || AWSS3Constants.CHUNK_SIZE }  
  get BUCKET()       { return this._BUCKET }
  get RETRY_COUNT()  { return this.parameters.RETRY_COUNT ||  AWSS3Constants.RETRY_COUNT }
  
  constructor(s3Connection,bucket,parameters,yadamuLogger) {
    this.s3Connection = s3Connection
	this._BUCKET = bucket
	this.parameters = parameters || {}
	this.yadamuLogger = yadamuLogger
	this.buffer = Buffer.allocUnsafe(this.CHUNK_SIZE);
	this.offset = 0;
	this.writeOperations = new Set();
  }
  
  retryOperation(retryCount) {
	return retryCount < this.RETRY_COUNT 
  }
	   
  createWriteStream(key) {
	// this.yadamuLogger.trace([this.constructor.name],`createWriteStream(${key})`)
  
    const params = { 
	  Bucket : this.BUCKET
	, Key    : key
	, Body   : new PassThrough()
    }	
    
	const streamFinished = new Promise((resolve,reject) => {
	  this.s3Connection.upload(params).send((err, data) => {
 	    if (err) {
          this.yadamuLogger.handleException([AWSS3Constants.DATABASE_VENDOR,'UPLOAD',key],`FAILED`);
		  reject(err)
	    } 
	    else {
          // this.yadamuLogger.trace([AWSS3Constants.DATABASE_VENDOR,'UPLOAD',key],`SUCCESS : File uploaded to "${data.Location}"`);
		  resolve(params.Key)
	      this.writeOperations.delete(streamFinished)
	    }
	    params.Body.destroy(err);
      })
    })	  
	this.writeOperations.add(streamFinished)
	return params.Body
  }

  async createBucketContainer() {
    let stack;
	let operation
    try {
      stack = new Error().stack
	  operation = `AWS.S3.headBucket(${this.BUCKET})`
	  try {
	    let results = await this.s3Connection.headBucket({Bucket : this.BUCKET}).promise();
      } catch (e) {
		if (e.statusCode === AWSS3Constants.HTTP_NAMED_STATUS_CODES.NOT_FOUND) {
          stack = new Error().stack
	      operation = `AWS.S3.createBucket(${this.BUCKET})`
    	  await this.s3Connection.createBucket({Bucket: this.BUCKET}).promise()
		  return
        } 
		throw e
	  }
	} catch (e) { 
      throw new AWSS3Error(e,stack,operation)
	}
  }

  async verifyBucketContainer() {
	 
	let stack;
    try {
      stack = new Error().stack
      let results = await this.s3Connection.headBucket({Bucket : this.BUCKET}).promise();
	} catch (e) { 
      throw new AWSS3Error(e,stack,`AWS.S3.headBucket(${this.BUCKET})`)
	}
  }
  
  async putObject(key,content,params) {
	params = params || {}
	params.Bucket = this.BUCKET
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
      throw new AWSS3Error(e,stack,`AWS.S3.putBucket(s3://${this.BUCKET}/${params.Key})`)
	}
  }
  
  async getObject(key,params) {
	params = params || {}
	params.Bucket = this.BUCKET
	params.Key = key
    const stack = new Error().stack
    try {
      const results = await this.s3Connection.getObject(params).promise()
	  return results;
    } catch (e) {
      throw new AWSS3Error(e,stack,`AWS.S3.getObject(s3://${this.BUCKET}/${params.Key})`)
	}
  }

  async getObjectProps(key,params) {
	 
	params = params || {}
	params.Bucket = this.BUCKET
	params.Key = key

	let retryCount =  0
    const stack = new Error().stack

	while (true) {
      try {
        return await this.s3Connection.headObject(params).promise()
	  } catch (e) {
	    const awsError = new AWSS3Error(e,stack,`AWS.S3.headObject(s3://${this.BUCKET}/${params.Key})`)
        if (awsError.possibleConsistencyError() && this.retryOperation(retryCount)) { 
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
	params.Bucket = this.BUCKET
	params.Key = key
    const stack = new Error().stack
    try {
      const stream = await this.s3Connection.getObject(params).createReadStream()
	  return stream;
    } catch (e) {
      throw new AWSS3Error(e,stack,`AWS.S3.getObject(s3://${this.BUCKET}/${params.Key})`)
	}
  }
  
  async deleteFolder(key,params) {
	 
	params = params || {}
	params.Bucket = this.BUCKET
	params.Prefix = key.split(path.sep).join(path.posix.sep)
	let stack
	let folder
    do { 
      try {
		stack = new Error().stack
	    folder = await this.s3Connection.listObjectsV2(params).promise();
      } catch (e) {
        throw new AWSS3Error(e,stack,`AWS.S3.listObjectsV2(s3://${this.BUCKET}/${params.Key})`)
	  }
	  if (folder.Contents.length === 0) break;
	  const deleteParams = {
		Bucket : this.BUCKET
	  , Delete : { Objects : folder.Contents.map((c) => { return {Key : c.Key }})}
	  }
	  try {
  	    stack = new Error().stack
	    await this.s3Connection.deleteObjects(deleteParams).promise();
      } catch (e) {
        throw new AWSS3Error(e,stack,`AWS.S3.deleteObjects(s3://${this.BUCKET}/${deleteParams.Delete.Objects.length})`)
	  }
	} while (folder.IsTruncated);
  }

}

module.exports = AWSS3StorageService;