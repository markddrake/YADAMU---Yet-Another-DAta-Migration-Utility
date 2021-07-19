"use strict" 

const path = require('path')

const AzureDBI = require('../../../YADAMU//loader/azure/azureDBI.js');
const AzureConstants = require('../../../YADAMU/loader/azure/azureConstants.js');

const YadamuTest = require('../../common/node/yadamuTest.js');

class AzureQA extends AzureDBI {
  
  static MIXINS = Object.freeze([path.resolve(__filename,'../../node/mixinCloudQA.js')])

  static #_YADAMU_DBI_PARAMETERS
	
  static get YADAMU_DBI_PARAMETERS()  { 
    this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,AzureConstants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[AzureConstants.DATABASE_KEY] || {},{RDBMS: AzureConstants.DATABASE_KEY}))
    return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
    return AzureQA.YADAMU_DBI_PARAMETERS
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
	  Object.assign(this.azureOptions,connectionProperties.yadamuOptions)
	  delete connectionProperties.yadamuOptions
	}
    super.setConnectionProperties(connectionProperties)
  }
  
  getContentLength(props) {
    return props.contentLength
  }

}
module.exports = AzureQA