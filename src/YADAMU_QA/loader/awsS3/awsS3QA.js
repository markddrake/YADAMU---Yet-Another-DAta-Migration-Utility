"use strict" 

const path = require('path')

const AWSS3DBI = require('../../../YADAMU//loader/awsS3/awsS3DBI.js');
const AWSS3Constants = require('../../../YADAMU/loader/awsS3/awsS3Constants.js');

const YadamuTest = require('../../common/node/yadamuTest.js');

class AWSS3QA extends AWSS3DBI {

  static MIXINS = Object.freeze([path.resolve(__filename,'../../node/mixinCloudQA.js')])

  static #_YADAMU_DBI_PARAMETERS
  
  static get YADAMU_DBI_PARAMETERS()  { 
	this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,AWSS3Constants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[AWSS3Constants.DATABASE_KEY] || {},{RDBMS: AWSS3Constants.DATABASE_KEY}))
	return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
    return AWSS3QA.YADAMU_DBI_PARAMETERS
  }	
	 	
  constructor(yadamu,settings,parameters) {
    super(yadamu,settings,parameters)
  }
  
  async initialize() {
    await super.initialize();
	if (this.options.recreateSchema === true) {
		await this.recreateSchema();
	}
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
}
 
module.exports = AWSS3QA