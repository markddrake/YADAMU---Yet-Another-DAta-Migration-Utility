"use strict"

const Stream = require('stream');
const path = require('path')
const Transform = require('stream').Transform

const AWSS3Constants = require('./awsS3Constants.js');
const AWSS3Error = require('./awsS3Error.js')

class AWSS3StorageService extends Transform {

  get CHUNK_SIZE()   { return this.parameters.CHUNK_SIZE  || AWSS3Constants.CHUNK_SIZE }  
  get BUCKET()       { return this._BUCKET }
  
  constructor(s3Connection,bucket,parameters,yadamuLogger) {
	super()
    this.s3Connection = s3Connection
	this._BUCKET = bucket
	this.parameters = parameters || {}
	this.yadamuLogger = yadamuLogger
	this.buffer = Buffer.allocUnsafe(this.CHUNK_SIZE);
	this.offset = 0;
  }
  
  createWriteStream(key) {
	// this.yadamuLogger.trace([this.constructor.name],`createWriteStream(${key})`)
  
    const params = { 
	  Bucket : this.BUCKET
	, Key    : key
	, Body   : new Stream.PassThrough()
    }	
	
	this.s3Connection.upload(params).send((err, data) => {
      if (err) {
		params.Body.destroy(err);
      } 
	  else {
        // console.log(`File uploaded and available at ${data.Location}`);
        params.Body.destroy();
      }
    }) 
	return params.Body
  }
  
  async verifyStorageTarget() {
	 
	let stack;
    try {
      stack = new Error().stack
	  try {
	    let results = await this.s3Connection.headBucket({Bucket : this.BUCKET}).promise();
      } catch (e) {
		if (e.statusCode === AWSS3Constants.HTTP_NAMED_STATUS_CODES.NOT_FOUND) {
          stack = new Error().stack
    	  await this.s3Connection.createBucket({Bucket: this.BUCKET}).promise()
		  return
        } 
		throw e
	  }
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
	       
  _transform(data,enc,callback) {
    if (this.offset + data.length > this.CHUNK_SIZE) {
      this.push.write(this.buffer.slice(0,this.offset),undefined,() => {
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

module.exports = AWSS3StorageService;