"use strict" 

const AzureDBI = require('../../../YADAMU//loader/azure/azureDBI.js');

class AzureQA extends AzureDBI {
  
  async recreateSchema() {
	await this.cloudService.deleteFolder(this.IMPORT_FOLDER)
  }    
	
  constructor(yadamu) {
     super(yadamu)
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
  
}
module.exports = AzureQA