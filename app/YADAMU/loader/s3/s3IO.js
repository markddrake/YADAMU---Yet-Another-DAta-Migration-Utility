"use strict"

const Stream = require('stream');

const Transform = require('stream').Transform
const S3Constants = require('./s3Constants.js');
const S3Error = require('./s3Error.js')

class S3IO extends Transform{

  get CHUNK_SIZE()   { return this.parameters.CHUNK_SIZE  || S3Constants.CHUNK_SIZE }  
  get BUCKET_NAME()  { return this.parameters.BUCKET_NAME || S3Constants.BUCKET_NAME }
  
  constructor(s3,parameters,yadamuLogger) {
	super()
    this.s3 = s3
	this.parameters = parameters || {}
	this.yadamuLogger = yadamuLogger
	this.buffer = Buffer.allocUnsafe(this.CHUNK_SIZE);
	this.offset = 0;
  }
  
  createWriteStream(key) {
	// this.yadamuLogger.trace([this.constructor.name],`createWriteStream(${key})`)
  
    const params = { 
	  Bucket : this.BUCKET_NAME
	, Key    : key
	, Body   : new Stream.PassThrough()
    }	
	
	this.s3.upload(params).send((err, data) => {
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
  
  async verifyBucket() {
	 
	let stack;
    try {
      stack = new Error().stack
	  try {
	    let results = await this.s3.headBucket({Bucket : this.BUCKET_NAME}).promise();
      } catch (e) {
		if (e.statusCode === S3Constants.HTTP_NAMED_STATUS_CODES.NOT_FOUND) {
          stack = new Error().stack
    	  await this.s3.createBucket({Bucket: this.BUCKET_NAME}).promise()
		  return
        } 
		throw e
	  }
	} catch (e) { 
      throw new S3Error(e,stack,`AWS.S3.headBucket(${this.BUCKET_NAME})`)
	}
  }
  
  async putObject(key,content,params) {
	params = params || {}
	params.Bucket = this.BUCKET_NAME
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
      const results = await this.s3.putObject(params).promise()
	  return results;
    } catch (e) {
	  console.log(e)
      throw new S3Error(e,stack,`AWS.S3.putBucket(s3://${this.BUCKET_NAME}/${params.Key})`)
	}
  }
  
  async getObject(key,params) {
	params = params || {}
	params.Bucket = this.BUCKET_NAME
	params.Key = key
    const stack = new Error().stack
    try {
      const results = await this.s3.getObject(params).promise()
	  return results;
    } catch (e) {
	  console.log(e)
      throw new S3Error(e,stack,`AWS.S3.getObject(s3://${this.BUCKET_NAME}/${params.Key})`)
	}
  }

  async createReadStream(key,params) {
	  
	// this.yadamuLogger.trace([this.constructor.name],`createReadStream(${key})`)
	
	params = params || {}
	params.Bucket = this.BUCKET_NAME
	params.Key = key
    const stack = new Error().stack
    try {
      const stream = await this.s3.getObject(params).createReadStream()
	  return stream;
    } catch (e) {
	  console.log(e)
      throw new S3Error(e,stack,`AWS.S3.getObject(s3://${this.BUCKET_NAME}/${params.Key})`)
	}
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

module.exports = S3IO;