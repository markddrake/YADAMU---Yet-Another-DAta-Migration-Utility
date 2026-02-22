
import path                           from 'path';

import { 
  performance 
}                                     from 'perf_hooks';

/* Database Vendors API */                                    

import                                https from 'https'
import { 
  NodeHttpHandler 
}                                     from '@aws-sdk/node-http-handler'
import { 
  S3Client
}                                     from "@aws-sdk/client-s3";

/* Yadamu Core */                                    
							          
import YadamuConstants                from '../../lib/yadamuConstants.js'
import YadamuLibrary                  from '../../lib/yadamuLibrary.js'

/* Yadamu DBI */                                    

import DatabaseError                  from './awsS3Exception.js';

import CloudDBI                       from '../cloud/cloudDBI.js';
import DBIConstants                   from '../base/dbiConstants.js'

/* Vendor Specific DBI Implimentation */                                    
							          							          
import AWSS3Constants                 from './awsS3Constants.js';
import AWSS3StorageService            from './awsS3StorageService.js';

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

  static #DBI_PARAMETERS

  static get DBI_PARAMETERS()  { 
	this.#DBI_PARAMETERS = this.#DBI_PARAMETERS || Object.freeze({
      ...DBIConstants.DBI_PARAMETERS
    , ...AWSS3Constants.DBI_PARAMETERS
    })
	return this.#DBI_PARAMETERS
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

  /*
  set REGION(v) { this._REGION = v }
  get REGION() {
    this._REGION = this._REGION || (() => { 
	  const region = this.parameters.REGION || this.CONNECTION_PROPERTIES.region   
	  return region
	})();
	return this._REGION
  }
  */
  
  get STORAGE_ID()            { return this.BUCKET }
  
  redactPasswords() {
	 const connectionProperties = super.redactPasswords()
	 connectionProperties.connection.credentials.secretAccessKey = '#REDACTED'
     return connectionProperties
  }
  
  addVendorExtensions(connectionProperties) {
	
	let url = connectionProperties.endpoint
	url = url && url.indexOf('://') < 0 ? `http://${url}` : url
	try {
	  url = new URL(url ? url : 'http://0.0.0.0')
	} catch (e) {
      this.LOGGER.error([this.DATABASE_VENDOR,'CONNECTION'],`Invalid endpoint specified: "${connectionProperties.endpoint}"`)
	  this.LOGGER.handleException([this.DATABASE_VENDOR,'CONNECTION'],e)
	  url = new URL('http://0.0.0.0')
	}

    url.protocol                          = this.parameters.PROTOCOL  || url.protocol 
	url.hostname                          = this.parameters.HOSTNAME  || url.hostname
	url.port                              = this.parameters.PORT      || url.port
	url                                   = url.toString()
	
	connectionProperties.accessKeyId      = this.parameters.USERNAME  || connectionProperties.accessKeyId 
    connectionProperties.secretAccessKey  = this.parameters.PASSWORD  || connectionProperties.secretAccessKey	
	connectionProperties.region           = this.parameters.REGION    || connectionProperties.region	         // this.REGION
    connectionProperties.s3ForcePathStyle = true
    connectionProperties.signatureVersion = "v4"
    connectionProperties.endpoint         = url
	
	
	connectionProperties.connection = {
      region             : connectionProperties.region
	, forcePathStyle     : connectionProperties.s3ForcePathStyle
	, endpoint           : connectionProperties.endpoint
	, signatureVersion   : connectionProperties.signatureVersion
	, credentials        : {
	    accessKeyId      : connectionProperties.accessKeyId
	  , secretAccessKey  : connectionProperties.secretAccessKey  
	  }
	}
	
	return connectionProperties

  }
  
  constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)

	this.DATABASE_ERROR_CLASS = DatabaseError
  }    
   
  getCredentials(vendorKey) {
	 
	switch (vendorKey) {
	  case 'snowflake':
	    return `AWS_KEY_ID = '${this.CONNECTION_PROPERTIES.accessKeyId}'  AWS_SECRET_KEY = '${this.CONNECTION_PROPERTIES.secretAccessKey}'`;
	}
	return ''
  }
  
  async createConnectionPool() {
  
    const agent = new https.Agent({
      keepAlive: true,
      maxSockets: 32,       // tune if needed
      keepAliveMsecs: 30000
    })
  
    this.cloudConnection = new S3Client({
      ...this.CONNECTION_PROPERTIES.connection,
      requestHandler: new NodeHttpHandler({
        httpsAgent: agent,
        connectionTimeout: 30000,
        socketTimeout: 300000
      })
    })
  
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
	     return `AWS_KEY_ID = '${this.CONNECTION_PROPERTIES.accessKeyId}'  AWS_SECRET_KEY = '${this.CONNECTION_PROPERTIES.secretAccessKey}'`;
	}
	return ''
  }
	  
}

export {AWSS3DBI as default }