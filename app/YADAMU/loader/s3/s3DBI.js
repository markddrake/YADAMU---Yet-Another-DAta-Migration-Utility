"use strict" 

const AWS = require('aws-sdk');
const path = require('path')
const Stream = require('stream')

const LoaderDBI = require('../node/LoaderDBI.js');
const S3Constants = require('./s3Constants.js');
const S3Writer = require('./s3Writer.js');
const CSVWriter = require('./csvWriter.js');
const S3Error = require('./s3Error.js');
const JSONParser = require('../../loader/node/jsonParser.js');
/*
**
** YADAMU Database Inteface class skeleton
**
*/

class S3DBI extends LoaderDBI {
 
  /*
  **
  ** Extends LoaderDBI enabling operations on Amazon Web Services S3 Buckets rather than a local file system.
  ** 
  ** !!! Make sure your head is wrapped around the following statements before touching this code.
  **
  ** An Export operaton involves reading data from the S3 object store
  ** An Import operation involves writing data to the S3 object store
  **
  */
  get WRITER() {
	return this.JSON_OUTPUT ? S3Writer : CSVWriter
  }  
 
  get DATABASE_VENDOR()     { return S3Constants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()     { return S3Constants.SOFTWARE_VENDOR};
  get BUCKET_NAME()         { return this.parameters.BUCKET_NAME || S3Constants.BUCKET_NAME }
  get CHUNK_SIZE()          { return this.parameters.CHUNK_SIZE  || S3Constants.CHUNK_SIZE }
  
  constructor(yadamu,exportFilePath) {
    // Export File Path is a Directory for in Load/Unload Mode
    super(yadamu,exportFilePath)
  }    
  
  async createConnectionPool() {
	// this.yadamuLogger.trace([this.constructor.name],`new AWS.S3()`)
	this.s3 = await new AWS.S3(this.connectionProperties)

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
      throw this.captureException(new S3Error(e,stack,`AWS.S3.headBucket(${this.BUCKET_NAME})`))
	}
  }
  
  async putObject(path,content,params) {
	params = params || {}
	params.Bucket = this.BUCKET_NAME
	params.Key = path
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
      throw this.captureException(new S3Error(e,stack,`AWS.S3.putBucket(s3://${this.BUCKET_NAME}/${params.Key})`))
	}
  }
  
  async getObject(path,params) {
	params = params || {}
	params.Bucket = this.BUCKET_NAME
	params.Key = path
    const stack = new Error().stack
    try {
      const results = await this.s3.getObject(params).promise()
	  return results;
    } catch (e) {
	  console.log(e)
      throw this.captureException(new S3Error(e,stack,`AWS.S3.getObject(s3://${this.BUCKET_NAME}/${params.Key})`))
	}
  }

  async getObjectStream(path,params) {
	params = params || {}
	params.Bucket = this.BUCKET_NAME
	params.Key = path
    const stack = new Error().stack
    try {
      const stream = await this.s3.getObject(params).createReadStream()
	  return stream;
    } catch (e) {
	  console.log(e)
      throw this.captureException(new S3Error(e,stack,`AWS.S3.getObject(s3://${this.BUCKET_NAME}/${params.Key})`))
	}
  }
  
  async loadMetadataFiles() {
    // this.yadamuLogger.trace([this.constructor.name,this.EXPORT_PATH],`loadMetadataFiles()`)
  	const metadata = {}
    if (this.controlFile) {
      const metdataRecords = await Promise.all(Object.keys(this.controlFile.metadata).map((tableName) => {
		return this.getObject(this.controlFile.metadata[tableName].file)
      }))
	  metdataRecords.forEach((content) =>  {
        const json = JSON.parse(content.Body.toString())
        metadata[json.tableName] = json;
      })
    }
    return metadata;      
  }
  
  /*
  **
  ** Remember: Import is Writing data to an S3 Object Store - unload.
  **
  */
  async writeMetadata() {
    const metadataFileList = {}
    const metadataObject = await Promise.all(Object.values(this.metadata).map((tableMetadata) => {
	     const file = `${path.join(this.metadataFolderPath,tableMetadata.tableName)}.json`.split(path.sep).join(path.posix.sep)
         metadataFileList[tableMetadata.tableName] = {file: file}
	     return this.putObject(file,tableMetadata);
    }))
	const dataFileList = {}
    Object.values(this.metadata).forEach((tableMetadata) =>  {
      dataFileList[tableMetadata.tableName] = {file: `${path.join(this.dataFolderPath,tableMetadata.tableName)}.${this.OUTPUT_FORMAT.toLowerCase()}`.split(path.sep).join(path.posix.sep)}
    })
	this.controlFile = { systemInformation : this.systemInformation, metadata : metadataFileList, data: dataFileList}
	await this.putObject(this.controlFilePath,this.controlFile)
  }
  
  async initializeImport() {
	 
    // this.yadamuLogger.trace([this.constructor.name],`initializeImport()`)
      	
	await this.verifyBucket()	

	this.uploadFolder = path.join(this.EXPORT_PATH,this.parameters.TO_USER).split(path.sep).join(path.posix.sep) 
	this.controlFilePath = `${path.join(this.uploadFolder,this.parameters.TO_USER)}.json`.split(path.sep).join(path.posix.sep) 
    this.metadataFolderPath = path.join(this.uploadFolder,'metadata').split(path.sep).join(path.posix.sep) 
    this.dataFolderPath = path.join(this.uploadFolder,'data').split(path.sep).join(path.posix.sep) 

    this.yadamuLogger.info(['Import',this.DATABASE_VENDOR],`Using control file "${this.BUCKET_NAME}/${this.controlFilePath}"`);

  }

  getOutputStream(tableName,ddlComplete) {
	// this.yadamuLogger.trace([this.constructor.name],`getOutputStream()`)
    const os = new this.WRITER(this,tableName,ddlComplete,this.status,this.yadamuLogger)  
    return os;
  }
  
  getFileOutputStream(tableName) {
	// this.yadamuLogger.trace([this.constructor.name],`getFileOutputStream(${this.controlFile.data[tableName].file})`)
  
    const params = { 
	  Bucket : this.BUCKET_NAME
	, Key    : this.controlFile.data[tableName].file
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
  
  /*
  **
  ** Remember: Export is Reading data from an S3 Object Store - load.
  **
  */

  async initializeExport() {
	// this.yadamuLogger.trace([this.constructor.name],`initializeExport()`)
	this.uploadFolder = path.join(this.EXPORT_PATH,this.parameters.FROM_USER).split(path.sep).join(path.posix.sep) 
    this.controlFilePath = `${path.join(this.uploadFolder,this.parameters.FROM_USER)}.json`.split(path.sep).join(path.posix.sep) 
    this.yadamuLogger.info(['Export',this.DATABASE_VENDOR],`Using control file "${this.BUCKET_NAME}/${this.controlFilePath}"`);
    const fileContents = await this.getObject(this.controlFilePath)
    this.controlFile = JSON.parse(fileContents.Body.toString())
  }

  async getInputStream(tableInfo) {
    // this.yadamuLogger.trace([this.DATABASE_VENDOR,tableInfo.TABLE_NAME],`Creating input stream on ${this.controlFile.data[tableInfo.TABLE_NAME].file}`)
    const jsonParser  = new JSONParser(this.yadamuLogger,this.MODE,this.controlFile.data[tableInfo.TABLE_NAME].file);
    const stream = await this.getObjectStream(this.controlFile.data[tableInfo.TABLE_NAME].file)
	return stream.pipe(jsonParser)
  }
  
  async setWorkerConnection() {
    // DBI implementations that do not use a pool / connection mechansim need to overide this function. eg MSSQLSERVER
	this.s3 = await this.manager.s3
  }
  
  classFactory(yadamu) {
	return new S3DBI(yadamu)
  }
    
}

module.exports = S3DBI
