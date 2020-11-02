"use strict" 

const AWS = require('aws-sdk');
const path = require('path')
const Stream = require('stream')

const CloudDBI = require('../node/cloudDBI.js');
const YadamuLibrary = require('../../../YADAMU/common/yadamuLibrary.js');
const AWSS3Constants = require('./awsS3Constants.js');
const AWSS3StorageService = require('./awsS3StorageService.js');

/*
**
** YADAMU Database Inteface class skeleton
**
*/

class AWSS3DBI extends CloudDBI {
 
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

  get DATABASE_VENDOR()     { return AWSS3Constants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()     { return AWSS3Constants.SOFTWARE_VENDOR};
  
  get STORAGE_ID() {
    this._BUCKET = this._BUCKET || (() => { 
	  const bucket = this.parameters.BUCKET || this.s3Options.bucket || AWSS3Constants.BUCKET
	  this._BUCKET = YadamuLibrary.macroSubstitions(bucket, this.yadamu.MACROS)
	  return this._BUCKET
	})();
	return this._BUCKET
  }
  
  constructor(yadamu) {
    super(yadamu)
	this.s3Options = {}
  }    
  
  getConnectionProperties() {
	  
    return {
	  accessKey        : this.parameters.USERNAME
    , secretAccessKey  : this.parameters.PASSWORD
	, endpoint         : `${this.parameters.HOSTNAME}:${this.parameters.PORT}`
    , s3ForcePathStyle : true
    , signatureVersion : "v4"
    }
	
  }
  
  async createConnectionPool() {
	// this.yadamuLogger.trace([this.constructor.name],`new AWS.S3()`)
	this.cloudConnection = await new AWS.S3(this.connectionProperties)
	this.cloudService = new AWSS3StorageService(this.cloudConnection,this.STORAGE_ID,{},this.yadamuLogger)
  }


  /*
  **
  ** Remember: Export is Reading data from an S3 Object Store - load.
  **
  */
  
  parseContents(fileContents) {
    return JSON.parse(fileContents.Body.toString())
  }

  classFactory(yadamu) {
	return new AWSS3DBI(yadamu)
  }
    
}

module.exports = AWSS3DBI
