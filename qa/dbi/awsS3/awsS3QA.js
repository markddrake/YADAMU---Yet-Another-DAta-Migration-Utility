"use strict" 

import AWSS3DBI          from '../../../node/dbi/awsS3/awsS3DBI.js';
import AWSS3Constants    from '../../../node/dbi/awsS3/awsS3Constants.js';

import YadamuTest        from '../../core/yadamu.js';
import YadamuQALibrary   from '../../lib/yadamuQALibrary.js'

class AWSS3QA extends YadamuQALibrary.loaderQAMixin(AWSS3DBI) {

  static #_YADAMU_DBI_PARAMETERS
  
  static get YADAMU_DBI_PARAMETERS()  { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,AWSS3Constants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[AWSS3Constants.DATABASE_KEY] || {},{RDBMS: AWSS3Constants.DATABASE_KEY}))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
    return AWSS3QA.YADAMU_DBI_PARAMETERS
  }	
	 	
  constructor(yadamu,manager,connectionSettings,parameters) {
    super(yadamu,manager,connectionSettings,parameters)
  }

  async initializeImport() {
    if (this.options.recreateSchema === true) {
 	  await this.recreateSchema();
	}
    await super.initializeImport();
  }	

  setConnectionProperties(connectionProperties) {
    if (connectionProperties.hasOwnProperty('yadamuOptions')) {
	  Object.assign(this.s3Options,connectionProperties.yadamuOptions)
	  delete connectionProperties.yadamuOptions
	}
    super.setConnectionProperties(connectionProperties)
  }
  
  getContentLength(props) {
    return props.ContentLength
  }

  classFactory(yadamu) {
    return new AWSS3QA(yadamu,this)
  }}
 
export { AWSS3QA as default }