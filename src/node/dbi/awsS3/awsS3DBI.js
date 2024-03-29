
import path                           from 'path';

import { 
  performance 
}                                     from 'perf_hooks';

/* Database Vendors API */                                    

import { 
  S3Client
}                                     from "@aws-sdk/client-s3";

/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

/* Yadamu DBI */                                    

import CloudDBI                       from '../cloud/cloudDBI.js';
import DBIConstants                   from '../base/dbiConstants.js'

/* Vendor Specific DBI Implimentation */                                    
							          							          
import AWSS3Constants                 from './awsS3Constants.js';
import AWSS3StorageService            from './awsS3StorageService.js';
import AWSS3Error                     from './awsS3Exception.js';

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

  static #_DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.DBI_PARAMETERS,AWSS3Constants.DBI_PARAMETERS))
	return this.#_DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
	return AWSS3DBI.DBI_PARAMETERS
  }

  get DATABASE_KEY()          { return AWSS3Constants.DATABASE_KEY};
  get DATABASE_VENDOR()       { return AWSS3Constants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()       { return AWSS3Constants.SOFTWARE_VENDOR};
  get PROTOCOL()              { return AWSS3Constants.PROTOCOL }
  
  get BUCKET() {
    this._BUCKET = this._BUCKET || (() => { 
	  const bucket = this.parameters.BUCKET || AWSS3Constants.BUCKET
	  return YadamuLibrary.macroSubstitions(bucket, this.yadamu.MACROS)
	})();
	return this._BUCKET
  }
  
  set REGION(v) { this._REGION = v }
  get REGION() {
    this._REGION = this._REGION || (() => { 
	  const region = this.parameters.REGION || this.vendorProperties.region   
	  return region
	})();
	return this._REGION
  }
  
  get STORAGE_ID()            { return this.BUCKET }
  
  constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)
  }    
   
  createDatabaseError(driverId,cause,stack,sql) {
    return new AWSS3Error(driverId,cause,stack,sql)
  }
  
  updateVendorProperties(vendorProperties) {
	
	let url = vendorProperties.endpoint
	url = url && url.indexOf('://') < 0 ? `http://${url}` : url
	try {
	  url = new URL(url ? url : 'http://0.0.0.0')
	} catch (e) {
      this.LOGGER.error([this.DATABASE_VENDOR,'CONNECTION'],`Invalid endpoint specified: "${vendorProperties.endpoint}"`)
	  this.LOGGER.handleException([this.DATABASE_VENDOR,'CONNECTION'],e)
	  url = new URL('http://0.0.0.0')
	}

    url.protocol                      = this.parameters.PROTOCOL  || url.protocol 
	url.hostname                      = this.parameters.HOSTNAME  || url.hostname
	url.port                          = this.parameters.PORT      || url.port
	url                               = url.toString()
	
	vendorProperties.accessKeyId      = this.parameters.USERNAME     || vendorProperties.accessKeyId 
    vendorProperties.secretAccessKey  = this.parameters.PASSWORD     || vendorProperties.secretAccessKey	
	vendorProperties.region           = this.REGION
    vendorProperties.s3ForcePathStyle = true
    vendorProperties.signatureVersion = "v4"
    vendorProperties.endpoint         = url
	
  }

  getCredentials(vendorKey) {
	 
	switch (vendorKey) {
	  case 'snowflake':
	    return `AWS_KEY_ID = '${this.vendorProperties.accessKeyId}'  AWS_SECRET_KEY = '${this.vendorProperties.secretAccessKey}'`;
	}
	return ''
  }
  
  async createConnectionPool() {
	// this.LOGGER.trace([this.constructor.name],`new S3Client()`)
	
	const s3ClientConfig = {
      region             : this.vendorProperties.region
	, forcePathStyle     : this.vendorProperties.s3ForcePathStyle
	, endpoint           : this.vendorProperties.endpoint
	, signatureVersion   : this.vendorProperties.signatureVersion
	, credentials        : {
	    accessKeyId      : this.vendorProperties.accessKeyId
	  , secretAccessKey  : this.vendorProperties.secretAccessKey  
	  }
	}
	
	this.cloudConnection = await new S3Client(s3ClientConfig)
	this.cloudService = new AWSS3StorageService(this,{})
  }


  /*
  **
  ** Remember: Export is Reading data from an S3 Object Store - load.
  **
  */

  getContentLength(props) {
    return props.ContentLength
  }

  classFactory(yadamu) {
	return new AWSS3DBI(yadamu,this,this.connectionParameters,this.parameters)
  }
   
  getCredentials(vendorKey) {
	 
	switch (vendorKey) {
	  case 'snowflake':
	     return `AWS_KEY_ID = '${this.vendorProperties.accessKeyId}'  AWS_SECRET_KEY = '${this.vendorProperties.secretAccessKey}'`;
	}
	return ''
  }
	  
}

export {AWSS3DBI as default }