"use strict" 

import AzureDBI          from '../../../node/dbi/azure/azureDBI.js';
import AzureConstants    from '../../../node/dbi/azure/azureConstants.js';

import YadamuTest        from '../../core/yadamu.js';
import YadamuQALibrary   from '../../lib/yadamuQALibrary.js'

class AzureQA extends YadamuQALibrary.loaderQAMixin(AzureDBI) {
  
  static #_DBI_PARAMETERS
	
  static get DBI_PARAMETERS()  { 
    this.#_DBI_PARAMETERS = this.#_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.DBI_PARAMETERS,AzureConstants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[AzureConstants.DATABASE_KEY] || {},{RDBMS: AzureConstants.DATABASE_KEY}))
    return this.#_DBI_PARAMETERS
  }
   
  get DBI_PARAMETERS() {
    return AzureQA.DBI_PARAMETERS
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
	  Object.assign(this.azureOptions,connectionProperties.yadamuOptions)
	  delete connectionProperties.yadamuOptions
	}
    super.setConnectionProperties(connectionProperties)
  }
  
  getContentLength(props) {
    return props.contentLength
  }

  
  classFactory(yadamu) {
    return new AzureQA(yadamu,this,this.connectionParameters,this.parameters)
  }
  
}
export { AzureQA as default }