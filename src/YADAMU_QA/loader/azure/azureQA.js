"use strict" 

import AzureDBI          from '../../../YADAMU//loader/azure/azureDBI.js';
import AzureConstants    from '../../../YADAMU/loader/azure/azureConstants.js';

import YadamuTest        from '../../common/node/yadamuTest.js';
import YadamuQALibrary   from '../../common/node/yadamuQALibrary.js'

class AzureQA extends YadamuQALibrary.loaderQAMixin(AzureDBI) {
  
  static #_YADAMU_DBI_PARAMETERS
	
  static get YADAMU_DBI_PARAMETERS()  { 
    this.#_YADAMU_DBI_PARAMETERS = this.#_YADAMU_DBI_PARAMETERS || Object.freeze(Object.assign({},YadamuTest.YADAMU_DBI_PARAMETERS,AzureConstants.DBI_PARAMETERS,YadamuTest.QA_CONFIGURATION[AzureConstants.DATABASE_KEY] || {},{RDBMS: AzureConstants.DATABASE_KEY}))
    return this.#_YADAMU_DBI_PARAMETERS
  }
   
  get YADAMU_DBI_PARAMETERS() {
    return AzureQA.YADAMU_DBI_PARAMETERS
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
    return new AzureQA(yadamu,this)
  }
  
}
export { AzureQA as default }