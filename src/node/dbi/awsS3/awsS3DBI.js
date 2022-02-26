"use strict" 

import path                from 'path'

import AWS                 from 'aws-sdk';

import CloudDBI            from '../cloud/cloudDBI.js';
import DBIConstants        from '../base/dbiConstants.js';
import YadamuConstants     from '../../lib/yadamuConstants.js';
import YadamuLibrary       from '../../lib/yadamuLibrary.js'

import AWSS3Constants      from './awsS3Constants.js';
import AWSS3StorageService from './awsS3StorageService.js';

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

  static #_YADAMU_DBI_PARAMETERS

  static get YADAMU_DBI_PARAMETERS()  { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},DBIConstants.YADAMU_DBI_PARAMETERS,AWSS3Constants.DBI_PARAMETERS))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
	return AWSS3DBI.YADAMU_DBI_PARAMETERS
  }

  get DATABASE_KEY()          { return AWSS3Constants.DATABASE_KEY};
  get DATABASE_VENDOR()       { return AWSS3Constants.DATABASE_VENDOR};
  get SOFTWARE_VENDOR()       { return AWSS3Constants.SOFTWARE_VENDOR};
  get PROTOCOL()              { return AWSS3Constants.PROTOCOL }
  
  get BUCKET() {
    this._BUCKET = this._BUCKET || (() => { 
	  const bucket = this.parameters.BUCKET || AWSS3Constants.BUCKET
	  this._BUCKET = YadamuLibrary.macroSubstitions(bucket, this.yadamu.MACROS)
	  return this._BUCKET
	})();
	return this._BUCKET
  }
  
  get STORAGE_ID()            { return this.BUCKET }
  
  constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)
  }    
   
  updateVendorProperties(vendorProperties) {
	
	let url = vendorProperties.endpoint
	url = url && url.indexOf('://') < 0 ? `http://${url}` : url
	try {
	  url = new URL(url ? url : 'http://0.0.0.0')
	} catch (e) {
      this.yadamuLogger.error([this.DATABASE_VENDOR,'CONNECTION'],`Invalid endpoint specified: "${vendorProperties.endpoint}"`)
	  this.yadamuLogger.handleException([this.DATABASE_VENDOR,'CONNECTION'],e)
	  url = new URL('http://0.0.0.0')
	}

    url.protocol                      = this.parameters.PROTOCOL  || url.protocol 
	url.hostname                      = this.parameters.HOSTNAME  || url.hostname
	url.port                          = this.parameters.PORT      || url.port
	url                               = url.toString()
	
    vendorProperties.accessKeyId       = this.parameters.USERNAME || vendorProperties.accessKeyId 
    vendorProperties.secretAccessKey  = this.parameters.PASSWORD  || vendorProperties.secretAccessKey	
	vendorProperties.region           = this.parameters.REGION    || vendorProperties.region
	vendorProperties.endpoint         = url
    vendorProperties.s3ForcePathStyle = true
    vendorProperties.signatureVersion = "v4"
    
  }

  getCredentials(vendorKey) {
	 
	switch (vendorKey) {
	  case 'snowflake':
	    return `AWS_KEY_ID = '${this.vendorProperties.accessKeyId}'  AWS_SECRET_KEY = '${this.vendorProperties.secretAccessKey}'`;
	}
	return ''
  }
  
  async createConnectionPool() {
	// this.yadamuLogger.trace([this.constructor.name],`new AWS.S3()`)
	this.cloudConnection = await new AWS.S3(this.vendorProperties)
	this.cloudService = new AWSS3StorageService(this,{})
  }


  /*
  **
  ** Remember: Export is Reading data from an S3 Object Store - load.
  **
  */
  
  parseJSON(fileContents) {
    return JSON.parse(fileContents.Body.toString())
  }

  classFactory(yadamu) {
	return new AWSS3DBI(yadamu,this)
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